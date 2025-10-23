#include "duckdb.hpp"

// JSON library
#include <nlohmann/json.hpp>

// Schema Registry Client includes
#include "schemaregistry/rest/ClientConfiguration.h"
#include "schemaregistry/rest/SchemaRegistryClient.h"
#include "schemaregistry/rest/model/Schema.h"
#include "schemaregistry/serdes/SerdeConfig.h"
#include "schemaregistry/serdes/SerdeTypes.h"
#include "schemaregistry/serdes/json/JsonSerializer.h"

#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "tributary_schema_registry.hpp"
#include "tributary_secrets.hpp"
namespace duckdb {

namespace {

void TributarySchemaRegistryPopulateAuth(schemaregistry::rest::ClientConfiguration &client_config,
                                         ClientContext &context, const string &schema_registry_url) {
	// Retrieve secret for authentication
	auto secret_match = TributaryGetSecretByPath(context, TRIBUTARY_SCHEMA_REGISTRY_SECRET_TYPE, schema_registry_url);
	if (secret_match.HasMatch()) {
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);
		Value basic_auth_username_val = kv_secret.TryGetValue("basic_auth_username");
		Value basic_auth_password_val = kv_secret.TryGetValue("basic_auth_password");
		Value bearer_access_token_val = kv_secret.TryGetValue("bearer_access_token");
		if (!basic_auth_username_val.IsNull() && !basic_auth_password_val.IsNull()) {
			std::string username = basic_auth_username_val.ToString();
			std::string password = basic_auth_password_val.ToString();
			client_config.setBasicAuth(make_pair(username, password));
		} else if (!bearer_access_token_val.IsNull()) {
			std::string token = bearer_access_token_val.ToString();
			client_config.setBearerAccessToken(token);
		}
	}
}

void TributarySchemaRegistrySerializeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &schema_registry_url_vector = args.data[0];
	auto &schema_vector = args.data[1];
	auto &topic_vector = args.data[2];
	auto &data_vector = args.data[3];

	GenericExecutor::ExecuteQuaternary<PrimitiveType<string_t>, PrimitiveType<string_t>, PrimitiveType<string_t>,
	                                   PrimitiveType<string_t>, PrimitiveType<string_t>>(
	    schema_registry_url_vector, schema_vector, topic_vector, data_vector, result, args.size(),
	    [&](PrimitiveType<string_t> schema_registry_url, PrimitiveType<string_t> schema_definition,
	        PrimitiveType<string_t> topic_name, PrimitiveType<string_t> data) {
		    // Create Schema Registry client configuration
		    auto client_config = std::make_shared<schemaregistry::rest::ClientConfiguration>(
		        std::vector<std::string> {schema_registry_url.val.GetString()});

		    // Retrieve secret for authentication
		    TributarySchemaRegistryPopulateAuth(*client_config, state.GetContext(),
		                                        schema_registry_url.val.GetString());

		    // Create Schema Registry client
		    std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client_ =
		        schemaregistry::rest::SchemaRegistryClient::newClient(client_config);

		    schemaregistry::rest::model::Schema schema;
		    schema.setSchemaType("JSON");
		    schema.setSchema(schema_definition.val.GetString());

		    std::unordered_map<std::string, std::string> rule_config;
		    schemaregistry::serdes::SerializerConfig ser_config(true, std::nullopt, true, true, rule_config);

		    std::unique_ptr<schemaregistry::serdes::json::JsonSerializer> serializer_ =
		        std::make_unique<schemaregistry::serdes::json::JsonSerializer>(client_, schema, nullptr, ser_config);

		    schemaregistry::serdes::SerializationContext ser_ctx;
		    ser_ctx.topic = topic_name.val.GetString();
		    ser_ctx.serde_type = schemaregistry::serdes::SerdeType::Value;
		    ser_ctx.serde_format = schemaregistry::serdes::SerdeFormat::Json;
		    ser_ctx.headers = std::nullopt;

		    nlohmann::json json_value = nlohmann::json::parse(data.val.GetString());

		    // Serialize the JSON object
		    std::vector<uint8_t> serialized_data = serializer_->serialize(ser_ctx, json_value);

		    auto vector_result = StringVector::AddStringOrBlob(
		        result, reinterpret_cast<const char *>(serialized_data.data()), serialized_data.size());
		    return vector_result;
	    });
}
} // namespace

void TributarySchemaRegistryAddFunctions(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    ScalarFunction("tributary_schema_registry_serialize_json",
	                   {LogicalType::VARCHAR, LogicalType::JSON(), LogicalType::VARCHAR, LogicalType::JSON()},
	                   LogicalType::BLOB, TributarySchemaRegistrySerializeFunction));
}

} // namespace duckdb
