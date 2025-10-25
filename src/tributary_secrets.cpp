#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"

#include "duckdb/main/secret/secret_manager.hpp"
#include "tributary_secrets.hpp"
#include "tributary_config.hpp"

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
		throw InvalidInputException("No scope set for schema registry secret: '%s'", input.type);
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
			throw InvalidInputException("Unknown parameter passed: " + lower_name);
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

static bool starts_with(const std::string &str, const std::string &prefix) {
	return str.size() >= prefix.size() && str.compare(0, prefix.size(), prefix) == 0;
}

unique_ptr<BaseSecret> TributaryClusterCreateSecretFunction(ClientContext &, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;

	auto scope = input.scope;
	if (scope.empty()) {
		throw InvalidInputException("No scope set for cluster secret: '%s'", input.type);
	}

	for (auto &scope_part : scope) {
		if (!starts_with(scope_part, "kafka://")) {
			throw InvalidInputException("Invalid scope part for cluster secret: '%s'. Must start with 'kafka://'",
			                            scope_part);
		}
	}

	auto result = make_uniq<KeyValueSecret>(scope, TRIBUTARY_CLUSTER_SECRET_TYPE, "config", input.name);

	std::unordered_set<string> existing_params;
	for (auto &named_param : TributaryConfigKeys()) {
		existing_params.insert(named_param);
	}

	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (existing_params.find(lower_name) == existing_params.end()) {
			throw InvalidInputException("Unknown parameter passed: " + lower_name);
		}
		result->secret_map[lower_name] = named_param.second;
	}

	result->redact_keys = {
	    // SASL / Authentication
	    "sasl.username",
	    "sasl.password",
	    "sasl.oauthbearer.token",
	    "sasl.jaas.config",

	    // SSL / TLS
	    "ssl.key.password",
	    "ssl.keystore.password",
	    "ssl.truststore.password",

	    // Optional: if keys/certs are embedded
	    "ssl.certificate.location",
	    "ssl.key.location",
	};

	return result;
}

void TributaryClusterSetSecretParameters(CreateSecretFunction &function) {
	for (const auto &config_key : TributaryConfigKeys()) {
		function.named_parameters[config_key] = LogicalType::VARCHAR;
	}
}

} // namespace

void TributaryCreateSecrets(ExtensionLoader &loader) {
	SecretType schema_registry_secret_type;
	schema_registry_secret_type.name = TRIBUTARY_SCHEMA_REGISTRY_SECRET_TYPE;
	schema_registry_secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	schema_registry_secret_type.default_provider = "config";

	loader.RegisterSecretType(schema_registry_secret_type);

	CreateSecretFunction schema_registry_create_function = {TRIBUTARY_SCHEMA_REGISTRY_SECRET_TYPE, "config",
	                                                        TributarySchemaRegistryCreateSecretFunction};
	TributarySchemaRegistrySetSecretParameters(schema_registry_create_function);
	loader.RegisterFunction(schema_registry_create_function);

	SecretType cluster_secret_type;
	cluster_secret_type.name = TRIBUTARY_CLUSTER_SECRET_TYPE;
	cluster_secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	cluster_secret_type.default_provider = "config";

	loader.RegisterSecretType(cluster_secret_type);

	CreateSecretFunction cluster_create_function = {TRIBUTARY_CLUSTER_SECRET_TYPE, "config",
	                                                TributaryClusterCreateSecretFunction};
	TributaryClusterSetSecretParameters(cluster_create_function);
	loader.RegisterFunction(cluster_create_function);
}

} // namespace duckdb