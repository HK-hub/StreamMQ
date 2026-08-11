package io.github.streammq.adapter.redisson.filter.expression;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL92 表达式解析器。
 *
 * <p>参考 RocketMQ 的表达式解析实现，支持：
 *
 * <ul>
 *   <li>比较操作：=, !=, >, >=, <, <=
 *   <li>逻辑操作：AND, OR, NOT
 *   <li>NULL 判断：IS NULL, IS NOT NULL
 *   <li>括号分组
 *   <li>字符串常量（单引号包裹）
 *   <li>数字常量
 * </ul>
 *
 * <p>表达式在构造时解析一次，编译为表达式树。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class SelectorParser {

  private static final Logger LOG = LoggerFactory.getLogger(SelectorParser.class);

  private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*");
  private static final Pattern STRING_PATTERN = Pattern.compile("^'([^']*)'");
  private static final Pattern NUMBER_PATTERN = Pattern.compile("^-?\\d+(\\.\\d+)?");

  private final String expression;
  private int pos;

  public SelectorParser(String expression) {
    this.expression = Objects.nonNull(expression) ? expression.trim() : "";
    this.pos = 0;
  }

  public Expression parse() {
    if (expression.isEmpty()) {
      return null;
    }
    pos = 0;
    return parseOr();
  }

  private Expression parseOr() {
    Expression left = parseAnd();
    skipWhitespace();
    while (matchIgnoreCase("OR")) {
      skipWhitespace();
      Expression right = parseAnd();
      left = new LogicExpression(left, right, LogicExpression.LogicType.OR);
      skipWhitespace();
    }
    return left;
  }

  private Expression parseAnd() {
    Expression left = parseNot();
    skipWhitespace();
    while (matchIgnoreCase("AND")) {
      skipWhitespace();
      Expression right = parseNot();
      left = new LogicExpression(left, right, LogicExpression.LogicType.AND);
      skipWhitespace();
    }
    return left;
  }

  private Expression parseNot() {
    if (matchIgnoreCase("NOT")) {
      skipWhitespace();
      Expression child = parsePrimary();
      return new LogicExpression(child, LogicExpression.LogicType.NOT);
    }
    return parsePrimary();
  }

  private Expression parsePrimary() {
    skipWhitespace();
    if (pos >= expression.length()) {
      return null;
    }

    char ch = expression.charAt(pos);

    if (ch == '(') {
      pos++;
      Expression expr = parseOr();
      skipWhitespace();
      if (pos < expression.length() && expression.charAt(pos) == ')') {
        pos++;
      }
      return expr;
    }

    String identifier = tryParseIdentifier();
    if (Objects.nonNull(identifier)) {
      skipWhitespace();
      return parseComparison(identifier);
    }

    return null;
  }

  private Expression parseComparison(String identifier) {
    skipWhitespace();
    if (pos >= expression.length()) {
      return null;
    }

    char ch = expression.charAt(pos);
    String next = peek(2);

    if (next.equalsIgnoreCase("IS")) {
      pos += 2;
      skipWhitespace();
      if (matchIgnoreCase("NULL")) {
        return new NullExpression(new PropertyExpression(identifier), true);
      } else if (matchIgnoreCase("NOT")) {
        skipWhitespace();
        matchIgnoreCase("NULL");
        return new NullExpression(new PropertyExpression(identifier), false);
      }
    }

    CompareExpression.CompareType compareType = null;

    if (next.equalsIgnoreCase("!=")) {
      compareType = CompareExpression.CompareType.NOT_EQUAL;
      pos += 2;
    } else if (next.equalsIgnoreCase(">=")) {
      compareType = CompareExpression.CompareType.GREATER_EQUAL;
      pos += 2;
    } else if (next.equalsIgnoreCase("<=")) {
      compareType = CompareExpression.CompareType.LESS_EQUAL;
      pos += 2;
    } else if (ch == '>') {
      compareType = CompareExpression.CompareType.GREATER_THAN;
      pos++;
    } else if (ch == '<') {
      compareType = CompareExpression.CompareType.LESS_THAN;
      pos++;
    } else if (ch == '=') {
      compareType = CompareExpression.CompareType.EQUAL;
      pos++;
    }

    if (Objects.nonNull(compareType)) {
      skipWhitespace();
      ConstantExpression constant = parseConstant();
      if (Objects.nonNull(constant)) {
        return new CompareExpression(new PropertyExpression(identifier), constant, compareType);
      }
    }

    return null;
  }

  private ConstantExpression parseConstant() {
    String stringValue = tryParseString();
    if (Objects.nonNull(stringValue)) {
      return new ConstantExpression(stringValue);
    }

    String numberValue = tryParseNumber();
    if (Objects.nonNull(numberValue)) {
      return new ConstantExpression(numberValue);
    }

    return null;
  }

  private String tryParseIdentifier() {
    Matcher matcher = IDENTIFIER_PATTERN.matcher(expression.substring(pos));
    if (matcher.find()) {
      String id = matcher.group();
      pos += id.length();
      return id;
    }
    return null;
  }

  private String tryParseString() {
    Matcher matcher = STRING_PATTERN.matcher(expression.substring(pos));
    if (matcher.find()) {
      String str = matcher.group(1);
      pos += matcher.group().length();
      return str;
    }
    return null;
  }

  private String tryParseNumber() {
    Matcher matcher = NUMBER_PATTERN.matcher(expression.substring(pos));
    if (matcher.find()) {
      String num = matcher.group();
      pos += num.length();
      return num;
    }
    return null;
  }

  private boolean matchIgnoreCase(String token) {
    if (pos + token.length() > expression.length()) {
      return false;
    }
    String substring = expression.substring(pos, pos + token.length());
    if (substring.equalsIgnoreCase(token)) {
      if (pos + token.length() == expression.length()) {
        pos += token.length();
        return true;
      }
      char nextChar = expression.charAt(pos + token.length());
      if (isWordBoundary(nextChar)) {
        pos += token.length();
        return true;
      }
    }
    return false;
  }

  private boolean isWordBoundary(char ch) {
    return Character.isWhitespace(ch)
        || ch == '('
        || ch == ')'
        || ch == '='
        || ch == '>'
        || ch == '<'
        || ch == '!'
        || ch == '\'';
  }

  private String peek(int length) {
    if (pos + length > expression.length()) {
      return expression.substring(pos);
    }
    return expression.substring(pos, pos + length);
  }

  private void skipWhitespace() {
    while (pos < expression.length() && Character.isWhitespace(expression.charAt(pos))) {
      pos++;
    }
  }

  /**
   * 构建表达式（静态工厂方法）。
   *
   * @param expression 表达式字符串
   * @return 表达式节点
   */
  public static Expression build(String expression) {
    if (Objects.isNull(expression)
        || expression.trim().isEmpty()
        || "*".equals(expression.trim())) {
      return null;
    }
    try {
      return new SelectorParser(expression).parse();
    } catch (Exception e) {
      LOG.warn("Failed to parse expression: {}", expression, e);
      return null;
    }
  }
}
