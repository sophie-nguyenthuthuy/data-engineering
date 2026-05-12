#include "parser/lexer.h"
#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace qc {

static std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), ::toupper);
    return s;
}

static const std::unordered_map<std::string, TokenKind> KEYWORDS = {
    {"SELECT",   TokenKind::KW_SELECT},  {"FROM",     TokenKind::KW_FROM},
    {"WHERE",    TokenKind::KW_WHERE},   {"JOIN",     TokenKind::KW_JOIN},
    {"INNER",    TokenKind::KW_INNER},   {"LEFT",     TokenKind::KW_LEFT},
    {"RIGHT",    TokenKind::KW_RIGHT},   {"FULL",     TokenKind::KW_FULL},
    {"OUTER",    TokenKind::KW_OUTER},   {"ON",       TokenKind::KW_ON},
    {"AS",       TokenKind::KW_AS},      {"AND",      TokenKind::KW_AND},
    {"OR",       TokenKind::KW_OR},      {"NOT",      TokenKind::KW_NOT},
    {"BETWEEN",  TokenKind::KW_BETWEEN}, {"IN",       TokenKind::KW_IN},
    {"IS",       TokenKind::KW_IS},      {"NULL",     TokenKind::KW_NULL},
    {"TRUE",     TokenKind::KW_TRUE},    {"FALSE",    TokenKind::KW_FALSE},
    {"DISTINCT", TokenKind::KW_DISTINCT},{"GROUP",    TokenKind::KW_GROUP},
    {"BY",       TokenKind::KW_BY},      {"HAVING",   TokenKind::KW_HAVING},
    {"ORDER",    TokenKind::KW_ORDER},   {"ASC",      TokenKind::KW_ASC},
    {"DESC",     TokenKind::KW_DESC},    {"LIMIT",    TokenKind::KW_LIMIT},
    {"OFFSET",   TokenKind::KW_OFFSET},  {"NULLS",    TokenKind::KW_NULLS},
    {"FIRST",    TokenKind::KW_FIRST},   {"LAST",     TokenKind::KW_LAST},
    {"DATE",     TokenKind::KW_DATE},    {"CAST",     TokenKind::KW_CAST},
    {"COUNT",    TokenKind::KW_COUNT},   {"SUM",      TokenKind::KW_SUM},
    {"AVG",      TokenKind::KW_AVG},     {"MIN",      TokenKind::KW_MIN},
    {"MAX",      TokenKind::KW_MAX},
};

Lexer::Lexer(std::string input) : input_(std::move(input)) {}

void Lexer::skip_whitespace() {
    while (pos_ < input_.size()) {
        char c = input_[pos_];
        if (c == '\n') { line_++; pos_++; }
        else if (std::isspace(c)) { pos_++; }
        else if (c == '-' && pos_ + 1 < input_.size() && input_[pos_+1] == '-') {
            // Line comment
            while (pos_ < input_.size() && input_[pos_] != '\n') pos_++;
        }
        else break;
    }
}

Token Lexer::read_number() {
    size_t start = pos_;
    bool is_float = false;
    while (pos_ < input_.size() && std::isdigit(input_[pos_])) pos_++;
    if (pos_ < input_.size() && input_[pos_] == '.') {
        is_float = true;
        pos_++;
        while (pos_ < input_.size() && std::isdigit(input_[pos_])) pos_++;
    }
    return {is_float ? TokenKind::FLOAT_LIT : TokenKind::INT_LIT,
            input_.substr(start, pos_ - start), line_};
}

Token Lexer::read_string() {
    pos_++; // skip opening quote
    size_t start = pos_;
    while (pos_ < input_.size() && input_[pos_] != '\'') {
        if (input_[pos_] == '\n') line_++;
        pos_++;
    }
    std::string text = input_.substr(start, pos_ - start);
    if (pos_ < input_.size()) pos_++; // skip closing quote
    return {TokenKind::STRING_LIT, text, line_};
}

Token Lexer::read_ident_or_keyword() {
    size_t start = pos_;
    while (pos_ < input_.size() && (std::isalnum(input_[pos_]) || input_[pos_] == '_'))
        pos_++;
    std::string text = input_.substr(start, pos_ - start);
    std::string upper = to_upper(text);
    auto it = KEYWORDS.find(upper);
    if (it != KEYWORDS.end())
        return {it->second, upper, line_};
    return {TokenKind::IDENT, text, line_};
}

Token Lexer::read_token() {
    skip_whitespace();
    if (pos_ >= input_.size())
        return {TokenKind::END_OF_FILE, "", line_};

    char c = input_[pos_];

    if (std::isdigit(c)) return read_number();
    if (c == '\'')       return read_string();
    if (std::isalpha(c) || c == '_') return read_ident_or_keyword();

    pos_++;
    switch (c) {
    case '(': return {TokenKind::LPAREN,    "(", line_};
    case ')': return {TokenKind::RPAREN,    ")", line_};
    case ',': return {TokenKind::COMMA,     ",", line_};
    case '.': return {TokenKind::DOT,       ".", line_};
    case ';': return {TokenKind::SEMICOLON, ";", line_};
    case '*': return {TokenKind::STAR,      "*", line_};
    case '+': return {TokenKind::PLUS,      "+", line_};
    case '-': return {TokenKind::MINUS,     "-", line_};
    case '/': return {TokenKind::SLASH,     "/", line_};
    case '%': return {TokenKind::PERCENT,   "%", line_};
    case '=': return {TokenKind::EQ,        "=", line_};
    case '<':
        if (pos_ < input_.size() && input_[pos_] == '=') { pos_++; return {TokenKind::LE, "<=", line_}; }
        if (pos_ < input_.size() && input_[pos_] == '>') { pos_++; return {TokenKind::NEQ,"<>", line_}; }
        return {TokenKind::LT, "<", line_};
    case '>':
        if (pos_ < input_.size() && input_[pos_] == '=') { pos_++; return {TokenKind::GE, ">=", line_}; }
        return {TokenKind::GT, ">", line_};
    case '!':
        if (pos_ < input_.size() && input_[pos_] == '=') { pos_++; return {TokenKind::NEQ,"!=", line_}; }
        break;
    }
    throw std::runtime_error(std::string("Unexpected character: ") + c);
}

Token Lexer::next() {
    if (has_peek_) {
        has_peek_ = false;
        return peek_token_;
    }
    return read_token();
}

Token Lexer::peek() {
    if (!has_peek_) {
        peek_token_ = read_token();
        has_peek_ = true;
    }
    return peek_token_;
}

void Lexer::consume() { (void)next(); }

} // namespace qc
