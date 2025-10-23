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

struct TributarySchemaRegistryDeleteSubjectVersionBindData : public TableFunctionData {
	const string schema_registry_url;
	const string subject;
	const int32_t version;
	const bool permanent;
	bool did_call;

	explicit TributarySchemaRegistryDeleteSubjectVersionBindData(ClientContext &context,
	                                                             const string &schema_registry_url,
	                                                             const string &subject, const int32_t version,
	                                                             const bool permanent)
	    : schema_registry_url(std::move(schema_registry_url)), subject(std::move(subject)), version(version),
	      permanent(permanent), did_call(false) {
	}
};

unique_ptr<FunctionData> TributarySchemaRegistryDeleteSubjectVersionBindFunction(ClientContext &context,
                                                                                 TableFunctionBindInput &input,
                                                                                 vector<LogicalType> &return_types,
                                                                                 vector<string> &names) {

	auto schema_registry_url = input.inputs[0].GetValue<string>();
	auto subject = input.inputs[1].GetValue<string>();
	auto version = input.inputs[2].GetValue<int32_t>();
	auto permanent = input.inputs[3].GetValue<bool>();

	names.push_back("deleted_version");
	return_types.push_back(LogicalType(LogicalTypeId::INTEGER));

	return make_uniq<TributarySchemaRegistryDeleteSubjectVersionBindData>(context, std::move(schema_registry_url),
	                                                                      std::move(subject), version, permanent);
}

void TributarySchemaRegistryDeleteSubjectVersionFunction(ClientContext &context, TableFunctionInput &data,
                                                         DataChunk &output) {

	auto &bind_data = data.bind_data->CastNoConst<TributarySchemaRegistryDeleteSubjectVersionBindData>();
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

	auto deleted_version = client_->deleteSubjectVersion(bind_data.subject, bind_data.version, bind_data.permanent);

	idx_t row_idx = 0;
	output.SetCardinality(1);
	output.data[0].SetValue(row_idx, Value::INTEGER(deleted_version));

	output.Verify();
}

void TributarySchemaRegistryAddDeleteSubjectVersionFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(TableFunction(
	    "tributary_schema_registry_delete_subject_version",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BOOLEAN},
	    TributarySchemaRegistryDeleteSubjectVersionFunction, TributarySchemaRegistryDeleteSubjectVersionBindFunction));
}

} // namespace duckdb