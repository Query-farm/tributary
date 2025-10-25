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

struct TributarySchemaRegistryGetSchemaLatestBindData : public TableFunctionData {
	const string schema_registry_url;
	const string subject;
	bool did_call;

	explicit TributarySchemaRegistryGetSchemaLatestBindData(ClientContext &context, const string &schema_registry_url,
	                                                        const string &subject)
	    : schema_registry_url(std::move(schema_registry_url)), subject(std::move(subject)), did_call(false) {
	}
};

unique_ptr<FunctionData> TributarySchemaRegistryGetSchemaLatestBindFunction(ClientContext &context,
                                                                            TableFunctionBindInput &input,
                                                                            vector<LogicalType> &return_types,
                                                                            vector<string> &names) {

	auto schema_registry_url = input.inputs[0].GetValue<string>();
	auto subject = input.inputs[1].GetValue<string>();

	TributarySchemaRegistryAddRegisteredSchemaColumns(names, return_types);

	return make_uniq<TributarySchemaRegistryGetSchemaLatestBindData>(context, std::move(schema_registry_url),
	                                                                 std::move(subject));
}

void TributarySchemaRegistryGetSchemaLatestFunction(ClientContext &context, TableFunctionInput &data,
                                                    DataChunk &output) {

	auto &bind_data = data.bind_data->CastNoConst<TributarySchemaRegistryGetSchemaLatestBindData>();
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

	auto registered_result = client_->getLatestVersion(bind_data.subject);

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

std::string get_duckdb_type(const std::string &json_type) {
	if (json_type == "integer") {
		return "BIGINT";
	} else if (json_type == "number") {
		return "DOUBLE";
	} else if (json_type == "string") {
		return "VARCHAR";
	} else if (json_type == "boolean") {
		return "BOOLEAN";
	} else if (json_type == "null") {
		return "JSON"; // DuckDB represents null as JSON type
	}
	return "JSON"; // fallback
}

std::string resolve_multiple_types(const std::vector<std::string> &types) {
	// Filter out null types for consideration
	std::vector<std::string> non_null_types;
	for (const auto &t : types) {
		if (t != "null") {
			non_null_types.push_back(t);
		}
	}

	// If no non-null types or multiple non-null types, return JSON
	if (non_null_types.empty() || non_null_types.size() > 1) {
		return "JSON";
	}

	// Single non-null type (possibly with null), return that type
	return get_duckdb_type(non_null_types[0]);
}

nlohmann::json schema_to_structure(const nlohmann::json &schema) {
	// Handle empty or non-object schemas
	if (!schema.is_object()) {
		return "JSON";
	}

	// Check for union types (anyOf, oneOf, allOf)
	if (schema.contains("anyOf") || schema.contains("oneOf") || schema.contains("allOf")) {
		return "JSON";
	}

	// Get the type field
	if (!schema.contains("type")) {
		return "JSON";
	}

	auto type_field = schema["type"];

	// Handle array of types
	if (type_field.is_array()) {
		std::vector<std::string> types;
		for (const auto &t : type_field) {
			if (t.is_string()) {
				types.push_back(t.get<std::string>());
			}
		}

		// Check if any are complex types (object or array)
		for (const auto &t : types) {
			if (t == "object" || t == "array") {
				return "JSON";
			}
		}

		return resolve_multiple_types(types);
	}

	// Handle single type as string
	if (!type_field.is_string()) {
		return "JSON";
	}

	std::string type = type_field.get<std::string>();

	// Handle primitive types
	if (type == "integer" || type == "number" || type == "string" || type == "boolean" || type == "null") {
		return get_duckdb_type(type);
	}

	// Handle array type
	if (type == "array") {
		auto result = nlohmann::json::array();

		if (schema.contains("items")) {
			auto items_schema = schema["items"];
			auto item_structure = schema_to_structure(items_schema);
			result.push_back(item_structure);
		} else {
			// No items specified, default to JSON
			result.push_back("JSON");
		}

		return result;
	}

	// Handle object type
	if (type == "object") {
		auto result = nlohmann::json::object();

		if (schema.contains("properties")) {
			auto properties = schema["properties"];

			for (auto &[key, value] : properties.items()) {
				result[key] = schema_to_structure(value);
			}
		}

		return result;
	}

	// Unknown type
	return "JSON";
}

std::string json_schema_to_duckdb_structure(const std::string &schema_str) {
	try {
		// Parse the JSON schema string
		nlohmann::json schema = nlohmann::json::parse(schema_str);

		// Convert to DuckDB structure
		auto structure = schema_to_structure(schema);

		// Return as string
		return structure.dump();
	} catch (const nlohmann::json::parse_error &e) {
		throw std::runtime_error("Failed to parse JSON schema: " + std::string(e.what()));
	} catch (const std::exception &e) {
		throw std::runtime_error("Error converting schema: " + std::string(e.what()));
	}
}

void TributarySchemaRegisteryGetLatestSchemaJsonStructureFunction(DataChunk &args, ExpressionState &state,
                                                                  Vector &result) {
	auto &schema_registry_url_vector = args.data[0];
	auto &subject_vector = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    schema_registry_url_vector, subject_vector, result, args.size(),
	    [&](string_t &schema_registry_url_str, string_t &subject_str) -> string_t {
		    // Convert string_t to std::string
		    std::string schema_registry_url(schema_registry_url_str.GetData(), schema_registry_url_str.GetSize());
		    std::string subject(subject_str.GetData(), subject_str.GetSize());

		    // Create client configuration
		    auto client_config = std::make_shared<schemaregistry::rest::ClientConfiguration>(
		        std::vector<std::string> {schema_registry_url});
		    // Retrieve secret for authentication
		    TributarySchemaRegistryPopulateAuth(*client_config, state.GetContext(), schema_registry_url);

		    // Create Schema Registry client
		    std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client_ =
		        schemaregistry::rest::SchemaRegistryClient::newClient(client_config);

		    auto registered_result = client_->getLatestVersion(subject);
		    if (!registered_result.getSchemaType().has_value()) {
			    throw InvalidInputException("Schema type is not available for subject: " + subject);
		    }
		    if (registered_result.getSchemaType().value() != "JSON") {
			    throw InvalidInputException("Schema type is not JSON for subject: " + subject);
		    }
		    if (!registered_result.getSchema().has_value()) {
			    throw InvalidInputException("Schema definition is not available for subject: " + subject);
		    }
		    auto schema_definition = registered_result.getSchema().value();

		    // Now thats a JSON schema, we need to convert it to a DuckDB JSON structure
		    // definition.
		    return StringVector::AddString(result, json_schema_to_duckdb_structure(schema_definition));
	    });
}

void TributarySchemaRegistryAddGetSchemaLatestFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(TableFunction(
	    "tributary_schema_registry_get_schema_latest", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	    TributarySchemaRegistryGetSchemaLatestFunction, TributarySchemaRegistryGetSchemaLatestBindFunction));

	loader.RegisterFunction(ScalarFunction(
	    "tributary_schema_registry_get_schema_latest_json_structure", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::VARCHAR, TributarySchemaRegisteryGetLatestSchemaJsonStructureFunction, nullptr, nullptr, nullptr,
	    nullptr, LogicalType::INVALID, FunctionStability::CONSISTENT_WITHIN_QUERY));
}

} // namespace duckdb