#pragma once

#include "tributary_extension.hpp"
#include "schemaregistry/rest/SchemaRegistryClient.h"

namespace duckdb {

void TributarySchemaRegistryAddFunctions(ExtensionLoader &loader);

void TributarySchemaRegistryPopulateAuth(schemaregistry::rest::ClientConfiguration &client_config,
                                         ClientContext &context, const string &schema_registry_url);

void TributarySchemaRegistryAddRegisteredSchemaColumns(vector<string> &names, vector<LogicalType> &types);

}; // namespace duckdb
