#pragma once

#include "duckdb.hpp"

namespace duckdb {

// A named parameter that is still passed by position only, so calls work as before.
inline FunctionParameter Positional(const char *name, LogicalType type) {
	return FunctionParameter(name, std::move(type), {}, FunctionParameterKind::POSITIONAL_ONLY);
}

// The signature of a table function, with named parameters.
inline FunctionSignature TableSignature(vector<FunctionParameter> parameters) {
	return FunctionSignature(std::move(parameters), LogicalType::INVALID);
}

class TributaryExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
