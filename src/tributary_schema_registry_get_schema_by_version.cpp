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

struct TributarySchemaRegistryGetSchemaByVersionBindData : public TableFunctionData {
	const string schema_registry_url;
	const string subject;
	const int32_t version;
	bool did_call;

	explicit TributarySchemaRegistryGetSchemaByVersionBindData(ClientContext &context,
	                                                           const string &schema_registry_url, const string &subject,
	                                                           const int32_t version)
	    : schema_registry_url(std::move(schema_registry_url)), subject(std::move(subject)), version(version),
	      did_call(false) {
	}
};

unique_ptr<FunctionData> TributarySchemaRegistryGetSchemaByVersionBindFunction(ClientContext &context,
                                                                               TableFunctionBindInput &input,
                                                                               vector<LogicalType> &return_types,
                                                                               vector<string> &names) {

	auto schema_registry_url = input.inputs[0].GetValue<string>();
	auto subject = input.inputs[1].GetValue<string>();
	auto version = input.inputs[2].GetValue<int32_t>();

	TributarySchemaRegistryAddRegisteredSchemaColumns(names, return_types);

	return make_uniq<TributarySchemaRegistryGetSchemaByVersionBindData>(context, std::move(schema_registry_url),
	                                                                    std::move(subject), version);
}

void TributarySchemaRegistryGetSchemaByVersionFunction(ClientContext &context, TableFunctionInput &data,
                                                       DataChunk &output) {

	auto &bind_data = data.bind_data->CastNoConst<TributarySchemaRegistryGetSchemaByVersionBindData>();
	if (bind_data.did_call) {
		output.SetCardinality(0);
		return;
	}
	bind_data.did_call = true;

	auto client_config = std::make_shared<schemaregistry::rest::ClientConfiguration>(
	    std::vector<std::string> {bind_data.schema_registry_url});
	// Retrieve secret for authentication
	TributarySchemaRegistryPopulateAuth(*client_config, context, bind_data.schema_registry_url);

	// Create Schema Registry client
	std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client_ =
	    schemaregistry::rest::SchemaRegistryClient::newClient(client_config);

	auto registered_result = client_->getVersion(bind_data.subject, bind_data.version);

	idx_t row_idx = 0;
	output.SetCardinality(1);
	output.data[0].SetValue(row_idx,
	                        registered_result.getId().has_value() ? Value(registered_result.getId().value()) : Value());
	output.data[1].SetValue(
	    row_idx, registered_result.getGuid().has_value() ? Value(registered_result.getGuid().value()) : Value());
	output.data[2].SetValue(
	    row_idx, registered_result.getSubject().has_value() ? Value(registered_result.getSubject().value()) : Value());
	output.data[3].SetValue(row_idx, registered_result.getVersion().has_value()
	                                     ? Value::INTEGER(registered_result.getVersion().value())
	                                     : Value());
	output.data[4].SetValue(row_idx, registered_result.getSchemaType().has_value()
	                                     ? Value(registered_result.getSchemaType().value())
	                                     : Value());
	output.data[5].SetValue(
	    row_idx, registered_result.getSchema().has_value() ? Value(registered_result.getSchema().value()) : Value());

	output.Verify();
}

void TributarySchemaRegistryAddGetSchemaByVersionFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(TableFunction("tributary_schema_registry_get_schema_by_version",
	                                      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER},
	                                      TributarySchemaRegistryGetSchemaByVersionFunction,
	                                      TributarySchemaRegistryGetSchemaByVersionBindFunction));
}

} // namespace duckdb