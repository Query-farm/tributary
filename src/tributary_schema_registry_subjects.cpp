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

struct TributarySchemaRegistrySubjectsBindData : public TableFunctionData {
	const string schema_registry_url;
	const bool deleted;
	vector<string> subjects;
	idx_t current_idx;
	bool did_call;

	explicit TributarySchemaRegistrySubjectsBindData(ClientContext &context, const string &schema_registry_url,
	                                                 const bool deleted)
	    : schema_registry_url(std::move(schema_registry_url)), deleted(deleted), current_idx(0), did_call(false) {
	}
};

unique_ptr<FunctionData> TributarySchemaRegistrySubjectsBindFunction(ClientContext &context,
                                                                     TableFunctionBindInput &input,
                                                                     vector<LogicalType> &return_types,
                                                                     vector<string> &names) {

	auto schema_registry_url = input.inputs[0].GetValue<string>();
	auto deleted = input.inputs.size() > 1 ? input.inputs[1].GetValue<bool>() : false;

	names.push_back("subject");
	return_types.push_back(LogicalType(LogicalTypeId::VARCHAR));

	return make_uniq<TributarySchemaRegistrySubjectsBindData>(context, std::move(schema_registry_url), deleted);
}

void TributarySchemaRegistrySubjectsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {

	auto &bind_data = data.bind_data->CastNoConst<TributarySchemaRegistrySubjectsBindData>();

	if (!bind_data.did_call) {
		bind_data.did_call = true;

		auto client_config = std::make_shared<schemaregistry::rest::ClientConfiguration>(
		    std::vector<std::string> {bind_data.schema_registry_url});
		// Retrieve secret for authentication
		TributarySchemaRegistryPopulateAuth(*client_config, context, bind_data.schema_registry_url);

		// Create Schema Registry client
		std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client_ =
		    schemaregistry::rest::SchemaRegistryClient::newClient(client_config);

		bind_data.subjects = client_->getAllSubjects(bind_data.deleted);
	}

	idx_t remaining_subjects = bind_data.subjects.size() - bind_data.current_idx;
	idx_t chunk_size = std::min(remaining_subjects, (idx_t)STANDARD_VECTOR_SIZE);

	if (chunk_size == 0) {
		output.SetCardinality(0);
		return;
	}

	output.SetCardinality(chunk_size);
	for (idx_t i = 0; i < chunk_size; i++) {
		output.data[0].SetValue(i, Value(bind_data.subjects[bind_data.current_idx + i]));
	}

	bind_data.current_idx += chunk_size;
	output.Verify();
}

void TributarySchemaRegistryAddGetSubjectsFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    TableFunction("tributary_schema_registry_subjects", {LogicalType::VARCHAR, LogicalType::BOOLEAN},
	                  TributarySchemaRegistrySubjectsFunction, TributarySchemaRegistrySubjectsBindFunction));
}

} // namespace duckdb