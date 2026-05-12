#pragma once
#include <string>
#include <string_view>
#include <vector>

namespace qc {

enum class TokenKind {
    // Literals
    INT_LIT, FLOAT_LIT, STRING_LIT, DATE_LIT,
    // Identifiers / Keywords
    IDENT,
    KW_SELECT, KW_FROM, KW_WHERE, KW_JOIN, KW_INNER, KW_LEFT, KW_RIGHT, KW_FULL,
    KW_OUTER, KW_ON, KW_AS, KW_AND, KW_OR, KW_NOT, KW_BETWEEN, KW_IN, KW_IS,
    KW_NULL, KW_TRUE, KW_FALSE, KW_DISTINCT, KW_GROUP, KW_BY, KW_HAVING,
    KW_ORDER, KW_ASC, KW_DESC, KW_LIMIT, KW_OFFSET, KW_NULLS, KW_FIRST, KW_LAST,
    KW_DATE, KW_CAST, KW_COUNT, KW_SUM, KW_AVG, KW_MIN, KW_MAX,
    // Punctuation
    LPAREN, RPAREN, COMMA, DOT, SEMICOLON, STAR,
    // Operators
    EQ, NEQ, LT, LE, GT, GE, PLUS, MINUS, SLASH, PERCENT,
    // Special
    END_OF_FILE,
};

struct Token {
    TokenKind   kind;
    std::string text;
    int         line{1};
};

class Lexer {
public:
    explicit Lexer(std::string input);

    Token next();
    Token peek();
    void  consume();

private:
    Token read_token();
    void  skip_whitespace();
    Token read_number();
    Token read_string();
    Token read_ident_or_keyword();

    std::string input_;
    size_t      pos_{0};
    int         line_{1};
    bool        has_peek_{false};
    Token       peek_token_;
};

} // namespace qc
