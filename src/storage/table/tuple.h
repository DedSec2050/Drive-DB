#pragma once

#include <string>
#include <variant>
#include <vector>
#include <cstring>
#include <cstdint>
#include <stdexcept>

namespace storage {

// ---------------------------------------------------------
// 1️⃣  Value: represents one cell (INT, TEXT, etc.)
// ---------------------------------------------------------
enum class ValueType : uint8_t {
    INT = 0,
    TEXT = 1
};

class Value {
public:
    Value() : type_(ValueType::TEXT), int_val_(0) {}
    explicit Value(int32_t v) : type_(ValueType::INT), int_val_(v) {}
    explicit Value(const std::string &v) : type_(ValueType::TEXT), text_val_(v), int_val_(0) {}

    ValueType type() const { return type_; }

    std::string to_string() const {
        if (type_ == ValueType::INT)
            return std::to_string(int_val_);
        else
            return text_val_;
    }

    int32_t as_int() const {
        if (type_ != ValueType::INT) throw std::runtime_error("Value is not INT");
        return int_val_;
    }

    std::string as_text() const {
        if (type_ != ValueType::TEXT) throw std::runtime_error("Value is not TEXT");
        return text_val_;
    }

    // ---------------------------------------------------------
    // Serialization (for writing to disk pages)
    // ---------------------------------------------------------
    std::vector<char> serialize() const {
        std::vector<char> buf;
        buf.push_back(static_cast<uint8_t>(type_));

        if (type_ == ValueType::INT) {
            int32_t val = int_val_;
            const char *ptr = reinterpret_cast<const char *>(&val);
            buf.insert(buf.end(), ptr, ptr + sizeof(int32_t));
        } else {
            uint16_t len = static_cast<uint16_t>(text_val_.size());
            const char *len_ptr = reinterpret_cast<const char *>(&len);
            buf.insert(buf.end(), len_ptr, len_ptr + sizeof(uint16_t));
            buf.insert(buf.end(), text_val_.begin(), text_val_.end());
        }

        return buf;
    }

    static Value deserialize(const char *&ptr) {
        ValueType type = static_cast<ValueType>(*ptr++);
        if (type == ValueType::INT) {
            int32_t val;
            std::memcpy(&val, ptr, sizeof(int32_t));
            ptr += sizeof(int32_t);
            return Value(val);
        } else {
            uint16_t len;
            std::memcpy(&len, ptr, sizeof(uint16_t));
            ptr += sizeof(uint16_t);
            std::string text(ptr, len);
            ptr += len;
            return Value(text);
        }
    }

private:
    ValueType type_;
    std::string text_val_;
    int32_t int_val_;
};

// ---------------------------------------------------------
// 2️⃣  Tuple: represents a row of multiple values
// ---------------------------------------------------------
class Tuple {
public:
    Tuple() = default;
    explicit Tuple(std::vector<Value> vals) : values_(std::move(vals)) {}

    const std::vector<Value> &values() const { return values_; }

    std::string to_string() const {
        std::string s;
        for (size_t i = 0; i < values_.size(); i++) {
            s += values_[i].to_string();
            if (i + 1 < values_.size()) s += ", ";
        }
        return s;
    }

    // ---------------------------------------------------------
    // Serialization for disk (called by HeapPage)
    // ---------------------------------------------------------
    std::vector<char> serialize() const {
        std::vector<char> buf;

        uint16_t num_values = static_cast<uint16_t>(values_.size());
        const char *num_ptr = reinterpret_cast<const char *>(&num_values);
        buf.insert(buf.end(), num_ptr, num_ptr + sizeof(uint16_t));

        for (const auto &val : values_) {
            auto val_bytes = val.serialize();
            buf.insert(buf.end(), val_bytes.begin(), val_bytes.end());
        }

        return buf;
    }

    static Tuple deserialize(const char *&ptr) {
        uint16_t num_values;
        std::memcpy(&num_values, ptr, sizeof(uint16_t));
        ptr += sizeof(uint16_t);

        std::vector<Value> vals;
        vals.reserve(num_values);

        for (int i = 0; i < num_values; i++) {
            vals.push_back(Value::deserialize(ptr));
        }

        return Tuple(vals);
    }

private:
    std::vector<Value> values_;
};

} // namespace storage
