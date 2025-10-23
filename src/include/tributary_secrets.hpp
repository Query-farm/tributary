#pragma once
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {
unique_ptr<SecretEntry> TributaryGetSecretByName(ClientContext &context, const string &secret_name);

#define TRIBUTARY_SCHEMA_REGISTRY_SECRET_TYPE "tributary_schema_registry"

SecretMatch TributaryGetSecretByPath(ClientContext &context, const string &secret_type, const string &path);

void TributaryCreateSecrets(ExtensionLoader &loader);

} // namespace duckdb
