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

struct TributarySchemaRegistryGetSchemaByGuidBindData : public TableFunctionData {
	const string schema_registry_url;
	const string guid;
	bool did_call;

	explicit TributarySchemaRegistryGetSchemaByGuidBindData(ClientContext &context, const string &schema_registry_url,
	                                                        const string &guid)
	    : schema_registry_url(std::move(schema_registry_url)), guid(std::move(guid)), did_call(false) {
	}
};

unique_ptr<FunctionData> TributarySchemaRegistryGetSchemaByGuidBindFunction(ClientContext &context,
                                                                            TableFunctionBindInput &input,
                                                                            vector<LogicalType> &return_types,
                                                                            vector<string> &names) {

	auto schema_registry_url = input.inputs[0].GetValue<string>();
	auto guid = input.inputs[1].GetValue<string>();

	names.push_back("schema_type");
	names.push_back("schema");
	return_types.push_back(LogicalType(LogicalTypeId::VARCHAR));
	return_types.push_back(LogicalType(LogicalTypeId::VARCHAR));

	return make_uniq<TributarySchemaRegistryGetSchemaByGuidBindData>(context, std::move(schema_registry_url),
	                                                                 std::move(guid));
}

void TributarySchemaRegistryGetSchemaByGuidFunction(ClientContext &context, TableFunctionInput &data,
                                                    DataChunk &output) {

	auto &bind_data = data.bind_data->CastNoConst<TributarySchemaRegistryGetSchemaByGuidBindData>();
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

	auto schema_result = client_->getByGuid(bind_data.guid);

	idx_t row_idx = 0;
	output.SetCardinality(1);
	output.data[0].SetValue(
	    row_idx, schema_result.getSchemaType().has_value() ? Value(schema_result.getSchemaType().value()) : Value());
	output.data[1].SetValue(row_idx,
	                        schema_result.getSchema().has_value() ? Value(schema_result.getSchema().value()) : Value());

	output.Verify();
}

void TributarySchemaRegistryAddGetSchemaByGuidFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(TableFunction(
	    "tributary_schema_registry_get_schema_by_guid", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	    TributarySchemaRegistryGetSchemaByGuidFunction, TributarySchemaRegistryGetSchemaByGuidBindFunction));
}

} // namespace duckdb