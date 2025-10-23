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

struct TributarySchemaRegistryRegisterSchemaBindData : public TableFunctionData {
	const string schema_registry_url;
	const string subject;
	const string schema_type;
	const string schema_definition;
	const bool normalize;
	bool did_call;

	explicit TributarySchemaRegistryRegisterSchemaBindData(ClientContext &context, const string &schema_registry_url,
	                                                       const string &subject, const string &schema_type,
	                                                       const string &schema_definition, const bool normalize)
	    : schema_registry_url(std::move(schema_registry_url)), subject(std::move(subject)),
	      schema_type(std::move(schema_type)), schema_definition(std::move(schema_definition)), normalize(normalize),
	      did_call(false) {
	}
};

unique_ptr<FunctionData> TributarySchemaRegistryRegisterSchemaBindFunction(ClientContext &context,
                                                                           TableFunctionBindInput &input,
                                                                           vector<LogicalType> &return_types,
                                                                           vector<string> &names) {

	auto schema_registry_url = input.inputs[0].GetValue<string>();
	auto subject = input.inputs[1].GetValue<string>();
	auto schema_type = input.inputs[2].GetValue<string>();
	auto schema_definition = input.inputs[3].GetValue<string>();
	auto normalize = input.inputs.size() > 4 ? input.inputs[4].GetValue<bool>() : false;

	TributarySchemaRegistryAddRegisteredSchemaColumns(names, return_types);

	return make_uniq<TributarySchemaRegistryRegisterSchemaBindData>(context, std::move(schema_registry_url),
	                                                                std::move(subject), std::move(schema_type),
	                                                                std::move(schema_definition), normalize);
}

void TributarySchemaRegistryRegisterSchemaFunction(ClientContext &context, TableFunctionInput &data,
                                                   DataChunk &output) {

	auto &bind_data = data.bind_data->CastNoConst<TributarySchemaRegistryRegisterSchemaBindData>();
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

	schemaregistry::rest::model::Schema schema;
	schema.setSchemaType(bind_data.schema_type);
	schema.setSchema(bind_data.schema_definition);

	auto registered_result = client_->registerSchema(bind_data.subject, schema, bind_data.normalize);

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

void TributarySchemaRegistryAddRegisterSchemaFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(TableFunction(
	    "tributary_schema_registry_register_schema",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN},
	    TributarySchemaRegistryRegisterSchemaFunction, TributarySchemaRegistryRegisterSchemaBindFunction));
}

} // namespace duckdb