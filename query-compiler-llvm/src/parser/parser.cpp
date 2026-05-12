#include "parser/parser.h"
#include "common/types.h"
#include <cassert>
#include <stdexcept>

namespace qc {

using namespace ast;

Parser::Parser(std::string sql) : lex_(std::move(sql)) {}

static std::string token_name(TokenKind k) {
    switch (k) {
    case TokenKind::IDENT:       return "IDENT";
    case TokenKind::INT_LIT:     return "INT";
    case TokenKind::FLOAT_LIT:   return "FLOAT";
    case TokenKind::STRING_LIT:  return "STRING";
    case TokenKind::END_OF_FILE: return "EOF";
    case TokenKind::COMMA:       return ",";
    case TokenKind::LPAREN:      return "(";
    case TokenKind::RPAREN:      return ")";
    case TokenKind::STAR:        return "*";
    default:                     return "?";
    }
}

Token Parser::expect(TokenKind kind) {
    Token t = lex_.next();
    if (t.kind != kind)
        throw ParseError("Expected " + token_name(kind) + " but got '" + t.text + "'");
    return t;
}

bool Parser::match(TokenKind kind) {
    if (lex_.peek().kind == kind) { lex_.consume(); return true; }
    return false;
}

bool Parser::peek_is(TokenKind kind) { return lex_.peek().kind == kind; }

// ─── SELECT statement ─────────────────────────────────────────────────────────

SelectStmt Parser::parse_select() {
    SelectStmt stmt;

    auto t = lex_.next();
    if (t.kind != TokenKind::KW_SELECT)
        throw ParseError("Expected SELECT");

    if (match(TokenKind::KW_DISTINCT)) stmt.distinct = true;

    // Select list
    do {
        stmt.select_list.push_back(parse_select_item());
    } while (match(TokenKind::COMMA));

    // FROM
    if (match(TokenKind::KW_FROM)) {
        do {
            stmt.from_list.push_back(parse_from_item());
        } while (match(TokenKind::COMMA));
    }

    // WHERE
    if (match(TokenKind::KW_WHERE))
        stmt.where_clause = parse_expr();

    // GROUP BY
    if (peek_is(TokenKind::KW_GROUP)) {
        lex_.consume();
        expect(TokenKind::KW_BY);
        do {
            stmt.group_by.push_back(parse_expr());
        } while (match(TokenKind::COMMA));
    }

    // HAVING
    if (match(TokenKind::KW_HAVING))
        stmt.having = parse_expr();

    // ORDER BY
    if (peek_is(TokenKind::KW_ORDER)) {
        lex_.consume();
        expect(TokenKind::KW_BY);
        do {
            SortKey sk;
            sk.expr = parse_expr();
            sk.ascending = true;
            if (peek_is(TokenKind::KW_DESC)) { lex_.consume(); sk.ascending = false; }
            else match(TokenKind::KW_ASC);
            stmt.order_by.push_back(std::move(sk));
        } while (match(TokenKind::COMMA));
    }

    // LIMIT
    if (match(TokenKind::KW_LIMIT)) {
        auto lt = expect(TokenKind::INT_LIT);
        stmt.limit = std::stoll(lt.text);
    }

    // OFFSET
    if (match(TokenKind::KW_OFFSET)) {
        auto ot = expect(TokenKind::INT_LIT);
        stmt.offset = std::stoll(ot.text);
    }

    match(TokenKind::SEMICOLON);
    return stmt;
}

SelectItem Parser::parse_select_item() {
    SelectItem item;
    // SELECT * special case
    if (peek_is(TokenKind::STAR)) {
        lex_.consume();
        item.expr = make_col("", "*");
        return item;
    }
    // table.* case
    if (peek_is(TokenKind::IDENT)) {
        Token id = lex_.peek();
        // look ahead for table.*
        lex_.consume();
        if (peek_is(TokenKind::DOT)) {
            lex_.consume();
            if (peek_is(TokenKind::STAR)) {
                lex_.consume();
                item.expr = make_col(id.text, "*");
                return item;
            }
            // table.col — reconstruct
            Token col = lex_.next();
            item.expr = make_col(id.text, col.text);
        } else {
            // just an identifier — parse rest as expression
            // We consumed the identifier; reconstruct as ColumnRef
            item.expr = std::make_unique<Expr>(ColumnRef{"", id.text});
            // check for binary operators etc — this is simplified,
            // but parse_primary handles it; for now check if there's an operator
            // Actually we need to feed back. Let's just finish expression parsing
            // for the simple ident case by checking peek
        }
    } else {
        item.expr = parse_expr();
        if (match(TokenKind::KW_AS)) {
            item.alias = lex_.next().text;
        } else if (peek_is(TokenKind::IDENT)) {
            item.alias = lex_.next().text;
        }
        return item;
    }

    // Check for alias
    if (match(TokenKind::KW_AS)) {
        item.alias = lex_.next().text;
    } else if (peek_is(TokenKind::IDENT)) {
        item.alias = lex_.next().text;
    }
    return item;
}

FromItem Parser::parse_from_item() {
    FromItem fi;
    Token name = expect(TokenKind::IDENT);
    fi.table.name = name.text;

    if (match(TokenKind::KW_AS)) fi.table.alias = lex_.next().text;
    else if (peek_is(TokenKind::IDENT)) fi.table.alias = lex_.next().text;

    // JOIN clauses
    while (peek_is(TokenKind::KW_JOIN) || peek_is(TokenKind::KW_INNER) ||
           peek_is(TokenKind::KW_LEFT)  || peek_is(TokenKind::KW_RIGHT)) {
        auto jc = std::make_unique<JoinClause>();
        if (peek_is(TokenKind::KW_LEFT)) {
            jc->type = JoinType::LEFT; lex_.consume();
            match(TokenKind::KW_OUTER);
        } else if (peek_is(TokenKind::KW_RIGHT)) {
            jc->type = JoinType::RIGHT; lex_.consume();
            match(TokenKind::KW_OUTER);
        } else if (peek_is(TokenKind::KW_INNER)) {
            jc->type = JoinType::INNER; lex_.consume();
        }
        expect(TokenKind::KW_JOIN);
        Token jname = expect(TokenKind::IDENT);
        jc->table.name = jname.text;
        if (match(TokenKind::KW_AS)) jc->table.alias = lex_.next().text;
        else if (peek_is(TokenKind::IDENT)) jc->table.alias = lex_.next().text;
        expect(TokenKind::KW_ON);
        jc->condition = parse_expr();
        fi.joins.push_back(std::move(jc));
    }

    return fi;
}

// ─── Expression parsing (Pratt-style recursive descent) ───────────────────────

ExprPtr Parser::parse_expr()            { return parse_or_expr(); }
ExprPtr Parser::parse_or_expr() {
    auto left = parse_and_expr();
    while (peek_is(TokenKind::KW_OR)) {
        lex_.consume();
        auto right = parse_and_expr();
        left = make_binop(BinOp::OR, std::move(left), std::move(right));
    }
    return left;
}

ExprPtr Parser::parse_and_expr() {
    auto left = parse_not_expr();
    while (peek_is(TokenKind::KW_AND)) {
        lex_.consume();
        auto right = parse_not_expr();
        left = make_binop(BinOp::AND, std::move(left), std::move(right));
    }
    return left;
}

ExprPtr Parser::parse_not_expr() {
    if (peek_is(TokenKind::KW_NOT)) {
        lex_.consume();
        auto e = parse_comparison();
        return std::make_unique<Expr>(UnaryExpr{UnaryExpr::Op::NOT, std::move(e)});
    }
    return parse_comparison();
}

ExprPtr Parser::parse_comparison() {
    auto left = parse_additive();

    // BETWEEN
    if (peek_is(TokenKind::KW_BETWEEN)) {
        lex_.consume();
        auto lo = parse_additive();
        expect(TokenKind::KW_AND);
        auto hi = parse_additive();
        return std::make_unique<Expr>(BetweenExpr{std::move(left), std::move(lo), std::move(hi)});
    }

    auto pk = lex_.peek().kind;
    BinOp op;
    bool is_cmp = true;
    switch (pk) {
    case TokenKind::EQ:  op = BinOp::EQ;  break;
    case TokenKind::NEQ: op = BinOp::NEQ; break;
    case TokenKind::LT:  op = BinOp::LT;  break;
    case TokenKind::LE:  op = BinOp::LE;  break;
    case TokenKind::GT:  op = BinOp::GT;  break;
    case TokenKind::GE:  op = BinOp::GE;  break;
    default:             is_cmp = false;  break;
    }
    if (is_cmp) {
        lex_.consume();
        auto right = parse_additive();
        return make_binop(op, std::move(left), std::move(right));
    }
    return left;
}

ExprPtr Parser::parse_additive() {
    auto left = parse_multiplicative();
    while (peek_is(TokenKind::PLUS) || peek_is(TokenKind::MINUS)) {
        BinOp op = peek_is(TokenKind::PLUS) ? BinOp::ADD : BinOp::SUB;
        lex_.consume();
        auto right = parse_multiplicative();
        left = make_binop(op, std::move(left), std::move(right));
    }
    return left;
}

ExprPtr Parser::parse_multiplicative() {
    auto left = parse_unary();
    while (peek_is(TokenKind::STAR) || peek_is(TokenKind::SLASH)) {
        BinOp op = peek_is(TokenKind::STAR) ? BinOp::MUL : BinOp::DIV;
        lex_.consume();
        auto right = parse_unary();
        left = make_binop(op, std::move(left), std::move(right));
    }
    return left;
}

ExprPtr Parser::parse_unary() {
    if (peek_is(TokenKind::MINUS)) {
        lex_.consume();
        auto e = parse_primary();
        return std::make_unique<Expr>(UnaryExpr{UnaryExpr::Op::NEG, std::move(e)});
    }
    return parse_primary();
}

ExprPtr Parser::parse_function_call(const std::string& name) {
    // Aggregate functions: COUNT(*), SUM(expr), etc.
    expect(TokenKind::LPAREN);
    std::string upper = name;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

    AggFunc agg;
    bool is_agg = false;
    if      (upper == "COUNT") { agg = AggFunc::COUNT; is_agg = true; }
    else if (upper == "SUM")   { agg = AggFunc::SUM;   is_agg = true; }
    else if (upper == "AVG")   { agg = AggFunc::AVG;   is_agg = true; }
    else if (upper == "MIN")   { agg = AggFunc::MIN;   is_agg = true; }
    else if (upper == "MAX")   { agg = AggFunc::MAX;   is_agg = true; }

    if (is_agg) {
        bool distinct = match(TokenKind::KW_DISTINCT);
        if (peek_is(TokenKind::STAR)) {
            lex_.consume();
            expect(TokenKind::RPAREN);
            return std::make_unique<Expr>(AggExpr{AggFunc::COUNT_STAR, distinct, nullptr});
        }
        auto arg = parse_expr();
        expect(TokenKind::RPAREN);
        return std::make_unique<Expr>(AggExpr{agg, distinct, std::move(arg)});
    }

    throw ParseError("Unknown function: " + name);
}

ExprPtr Parser::parse_primary() {
    Token t = lex_.peek();

    switch (t.kind) {
    case TokenKind::INT_LIT:
        lex_.consume();
        return make_literal(static_cast<int64_t>(std::stoll(t.text)));

    case TokenKind::FLOAT_LIT:
        lex_.consume();
        return make_literal(std::stod(t.text));

    case TokenKind::STRING_LIT:
        lex_.consume();
        return make_literal(std::string(t.text));

    case TokenKind::KW_TRUE:
        lex_.consume();
        return make_literal(true);

    case TokenKind::KW_FALSE:
        lex_.consume();
        return make_literal(false);

    case TokenKind::KW_NULL:
        lex_.consume();
        return make_literal(null_value());

    case TokenKind::KW_DATE: {
        lex_.consume();
        Token ds = expect(TokenKind::STRING_LIT);
        int32_t d = parse_date(ds.text);
        return std::make_unique<Expr>(Literal{Value{d}});
    }

    case TokenKind::KW_COUNT: case TokenKind::KW_SUM:
    case TokenKind::KW_AVG:   case TokenKind::KW_MIN: case TokenKind::KW_MAX:
        lex_.consume();
        return parse_function_call(t.text);

    case TokenKind::IDENT: {
        lex_.consume();
        // Function call?
        if (peek_is(TokenKind::LPAREN))
            return parse_function_call(t.text);
        // Qualified: table.col
        if (peek_is(TokenKind::DOT)) {
            lex_.consume();
            Token col = lex_.next();
            return make_col(t.text, col.text);
        }
        return make_col("", t.text);
    }

    case TokenKind::LPAREN: {
        lex_.consume();
        auto e = parse_expr();
        expect(TokenKind::RPAREN);
        return e;
    }

    default:
        throw ParseError("Unexpected token in expression: '" + t.text + "'");
    }
}

SelectStmt parse_sql(const std::string& sql) {
    Parser p(sql);
    return p.parse_select();
}

} // namespace qc
