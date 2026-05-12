#pragma once
#include "common/types.h"
#include <memory>
#include <cassert>
#include <cstring>

namespace qc {

// Typed columnar storage. Raw arrays, no boxing.
// In the JIT path, the compiler accesses these arrays directly via typed pointers.
class Column {
public:
    Column(std::string name, TypeTag type, size_t capacity = 0)
        : name_(std::move(name)), type_(type), size_(0), capacity_(0) {
        if (capacity) reserve(capacity);
    }

    const std::string& name() const { return name_; }
    TypeTag type() const { return type_; }
    size_t  size() const { return size_; }

    void reserve(size_t n) {
        if (n <= capacity_) return;
        size_t elem_sz = elem_size();
        uint8_t* buf = new uint8_t[n * elem_sz];
        if (data_ && size_) memcpy(buf, data_.get(), size_ * elem_sz);
        data_.reset(buf);
        capacity_ = n;
        // VARCHAR: separately manage string storage
        if (type_ == TypeTag::VARCHAR) {
            auto* sp = new std::string[n];
            for (size_t i = 0; i < std::min(size_, n); i++)
                sp[i] = std::move(string_data_[i]);
            string_data_.reset(sp);
        }
    }

    void push_back(const Value& v) {
        if (size_ == capacity_) reserve(std::max(size_t(16), capacity_ * 2));
        switch (type_) {
        case TypeTag::BOOL:    ptr<bool>()[size_]    = std::get<bool>(v); break;
        case TypeTag::INT32:   ptr<int32_t>()[size_] = std::get<int32_t>(v); break;
        case TypeTag::INT64:   ptr<int64_t>()[size_] = std::get<int64_t>(v); break;
        case TypeTag::FLOAT64: ptr<double>()[size_]  = std::get<double>(v); break;
        case TypeTag::DATE:    ptr<int32_t>()[size_] = std::get<int32_t>(v); break;
        case TypeTag::VARCHAR: string_data_[size_]   = std::get<std::string>(v); break;
        default: break;
        }
        size_++;
    }

    Value get(size_t i) const {
        assert(i < size_);
        switch (type_) {
        case TypeTag::BOOL:    return ptr<bool>()[i];
        case TypeTag::INT32:   return ptr<int32_t>()[i];
        case TypeTag::INT64:   return ptr<int64_t>()[i];
        case TypeTag::FLOAT64: return ptr<double>()[i];
        case TypeTag::DATE:    return ptr<int32_t>()[i];
        case TypeTag::VARCHAR: return string_data_[i];
        default:               return null_value();
        }
    }

    // Direct typed array access for JIT-compiled functions
    template<typename T> T* ptr()             { return reinterpret_cast<T*>(data_.get()); }
    template<typename T> const T* ptr() const { return reinterpret_cast<const T*>(data_.get()); }
    void* raw_ptr()             { return data_.get(); }
    const void* raw_ptr() const { return data_.get(); }
    std::string* string_ptr()             { return string_data_.get(); }
    const std::string* string_ptr() const { return string_data_.get(); }

private:
    size_t elem_size() const {
        switch (type_) {
        case TypeTag::BOOL:    return sizeof(bool);
        case TypeTag::INT32:
        case TypeTag::DATE:    return sizeof(int32_t);
        case TypeTag::INT64:   return sizeof(int64_t);
        case TypeTag::FLOAT64: return sizeof(double);
        case TypeTag::VARCHAR: return sizeof(void*); // placeholder, strings are separate
        default:               return 0;
        }
    }

    std::string  name_;
    TypeTag      type_;
    size_t       size_{0};
    size_t       capacity_{0};
    std::unique_ptr<uint8_t[]>   data_;
    std::unique_ptr<std::string[]> string_data_; // only used for VARCHAR
};

} // namespace qc
