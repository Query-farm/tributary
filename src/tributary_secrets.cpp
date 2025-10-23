#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/main/secret/secret_manager.hpp"
#include "tributary_secrets.hpp"

namespace duckdb {

unique_ptr<SecretEntry> TributaryGetSecretByName(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	// FIXME: this should be adjusted once the `GetSecretByName` API supports this
	// use case
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
	if (secret_entry) {
		return secret_entry;
	}
	secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
	if (secret_entry) {
		return secret_entry;
	}
	return nullptr;
}

SecretMatch TributaryGetSecretByPath(ClientContext &context, const string &secret_type, const string &path) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	return secret_manager.LookupSecret(transaction, path, secret_type);
}

// KeyValueSecret TributaryAuthTokenForLocation(ClientContext &context, const string &server_location, const string
// &secret_name) { 	if (!secret_name.empty()) { 		auto secret_entry = TributaryGetSecretByName(context,
// secret_name); 		if
// (!secret_entry) { 			throw BinderException("Secret with name \"%s\" not found", secret_name);
// 		}

// 		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
// 	}

// 	auto secret_match = TributaryGetSecretByPath(context, server_location);
// 	if (secret_match.HasMatch()) {
// 		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);

// 		Value input_val = kv_secret.TryGetValue("auth_token");
// 		if (!input_val.IsNull()) {
// 			return input_val.ToString();
// 		}
// 		return "";
// 	}
// 	return "";
// }

namespace {
unique_ptr<BaseSecret> TributarySchemaRegistryCreateSecretFunction(ClientContext &, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;

	auto scope = input.scope;
	if (scope.empty()) {
		throw InternalException("No scope set for schema registry secret: '%s'", input.type);
	}

	auto result = make_uniq<KeyValueSecret>(scope, TRIBUTARY_SCHEMA_REGISTRY_SECRET_TYPE, "config", input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "basic_auth_username") {
			result->secret_map["basic_auth_username"] = named_param.second.ToString();
		} else if (lower_name == "basic_auth_password") {
			result->secret_map["basic_auth_password"] = named_param.second.ToString();
		} else if (lower_name == "bearer_access_token") {
			result->secret_map["bearer_access_token"] = named_param.second.ToString();
		} else {
			throw InternalException("Unknown parameter passed: " + lower_name);
		}
	}

	//! Set redact keys
	result->redact_keys = {"basic_auth_password", "bearer_access_token"};

	return result;
}

void TributarySchemaRegistrySetSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["basic_auth_username"] = LogicalType::VARCHAR;
	function.named_parameters["basic_auth_password"] = LogicalType::VARCHAR;
	function.named_parameters["bearer_access_token"] = LogicalType::VARCHAR;
}

} // namespace

void TributaryCreateSecrets(ExtensionLoader &loader) {
	SecretType secret_type;
	secret_type.name = TRIBUTARY_SCHEMA_REGISTRY_SECRET_TYPE;
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	CreateSecretFunction tributary_schema_registry_function = {TRIBUTARY_SCHEMA_REGISTRY_SECRET_TYPE, "config",
	                                                           TributarySchemaRegistryCreateSecretFunction};
	TributarySchemaRegistrySetSecretParameters(tributary_schema_registry_function);
	loader.RegisterFunction(tributary_schema_registry_function);
}

} // namespace duckdb