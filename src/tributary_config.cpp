#include "tributary_config.hpp"

#include <librdkafka/rdkafkacpp.h>

namespace duckdb {

std::vector<std::string> TributaryConfigKeys() {
	std::vector<std::string> config_keys;

	auto additional_config_keys = {"bootstrap.servers",
	                               "security.protocol",
	                               "sasl.mechanism",
	                               "sasl.username",
	                               "sasl.password",
	                               "ssl.ca.location",
	                               "ssl.certificate.location",
	                               "ssl.key.location",
	                               "ssl.key.password",
	                               "group.id",
	                               "client.id",
	                               "transactional.id",
	                               "schema.registry.url",
	                               "schema.registry.basic.auth.user.info",
	                               "debug",
	                               "sasl.oauthbearer.client.id",
	                               "sasl.oauthbearer.client.secret",
	                               "sasl.oauthbearer.method",
	                               "sasl.oauthbearer.token.endpoint.url"};

	for (auto &key : additional_config_keys) {
		config_keys.push_back(key);
	}

	std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	// Get all configuration properties
	std::list<std::string> *dump = conf->dump();

	// Extract just the parameter names (every other element starting from index 0)
	std::vector<std::string> param_names;
	bool is_key = true;

	for (auto it = dump->begin(); it != dump->end(); ++it) {
		if (is_key) {
			config_keys.push_back(*it);
			//			metadata_function.named_parameters[*it] = LogicalType(LogicalTypeId::VARCHAR);
		}
		is_key = !is_key;
	}

	return config_keys;
}

} // namespace duckdb