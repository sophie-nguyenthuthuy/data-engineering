#pragma once
#include "parser/lexer.h"
#include "parser/ast.h"
#include <stdexcept>

namespace qc {

class ParseError : public std::runtime_error {
public:
    explicit ParseError(const std::string& msg) : std::runtime_error(msg) {}
};

class Parser {
public:
    explicit Parser(std::string sql);

    ast::SelectStmt parse_select();

private:
    ast::SelectItem    parse_select_item();
    ast::FromItem      parse_from_item();
    ast::ExprPtr       parse_expr();
    ast::ExprPtr       parse_or_expr();
    ast::ExprPtr       parse_and_expr();
    ast::ExprPtr       parse_not_expr();
    ast::ExprPtr       parse_comparison();
    ast::ExprPtr       parse_additive();
    ast::ExprPtr       parse_multiplicative();
    ast::ExprPtr       parse_unary();
    ast::ExprPtr       parse_primary();
    ast::ExprPtr       parse_function_call(const std::string& name);

    Token expect(TokenKind kind);
    bool  match(TokenKind kind);
    bool  peek_is(TokenKind kind);
    bool  peek_keyword(const std::string& kw);

    Lexer lex_;
};

// Convenience: parse SQL string directly
ast::SelectStmt parse_sql(const std::string& sql);

} // namespace qc
