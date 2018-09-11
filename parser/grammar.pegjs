/**
 * sqlite-parser utilities
 */

{
  function makeArray(arr) {
    if (!isOkay(arr)) {
      return [];
    }
    return !Array.isArray(arr) ? [arr] : arr;
  }

  function isOkay(obj) {
    return obj != null;
  }

  function foldString(parts, glue = ' ') {
    const folded = parts
    .filter((part) => isOkay(part))
    .reduce((prev, cur) => {
      return `${prev}${nodeToString(cur)}${glue}`;
    }, '');
    return folded.trim();
  }

  function foldStringWord(parts) {
    return foldString(parts, '');
  }

  function foldStringKey(parts) {
    return foldString(parts).toLowerCase();
  }

  function flattenAll(arr) {
    return arr
    .filter((part) => isOkay(part))
    .reduce((prev, cur) => prev.concat(cur), []);
  }

  function unescape(str, quoteChar = '\'') {
    const re = new RegExp(`${quoteChar}{2}`, 'g');
    return nodeToString(str).replace(re, quoteChar);
  }

  function nodeToString(node = []) {
    return makeArray(node).join('');
  }

  /*
   * A text node has
   * - no leading or trailing whitespace
   */
  function textNode(node) {
    return nodeToString(node).trim();
  }

  function keyNode(node) {
    return textNode(node).toLowerCase();
  }

  function isArrayOkay(arr) {
    return Array.isArray(arr) && arr.length > 0 && isOkay(arr[0]);
  }

  function composeBinary(first, rest) {
    return rest
    .reduce((left, [ x, operation, y, right ]) => {
      return {
        'type': 'expression',
        'format': 'binary',
        'variant': 'operation',
        'operation': keyNode(operation),
        'left': left,
        'right': right
      };
    }, first);
  }
}

/**
 * sqlite-parser grammar
 */

/* Start Grammar */
start
  = o semi_optional s:( stmt_list )? semi_optional {
    return s;
  }

start_streaming
  = o semi_optional s:( stmt ) semi_optional  {
    return s;
  }

stmt_list
  = f:( stmt ) o b:( stmt_list_tail )* {
    return {
      'type': 'statement',
      'variant': 'list',
      'statement': flattenAll([ f, b ])
    };
  }

semi_optional
  = ( sym_semi )*

semi_required
  = ( sym_semi )+

/**
 * @note
 *   You need semicolon between multiple statements, otherwise can omit last
 *   semicolon in a group of statements.
 */
stmt_list_tail
  = semi_required s:( stmt ) o
  { return s; }

/**
 * Type definitions
 */
type_definition "Type Definition"
  = t:( type_definition_types / datatype_custom ) o a:( type_definition_args )? {
    return Object.assign(t, a);
  }

type_definition_types
  = n:( datatype_types ) {
    return {
      'type': 'datatype',
      'variant': n[0],
      'affinity': n[1]
    };
  }

/**
 * @note
 *   SQLite allows you to enter basically anything you want for a datatype
 *   because it doesn't enforce types you provide. The rules are as follows:
 *   1) If the declared type contains the string "INT" then it is assigned
 *   INTEGER affinity.
 *   2) If the declared type of the column contains any of the strings "CHAR",
 *   "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type
 *   VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
 *   3) If the declared type for a column contains the string "BLOB" or if no
 *   type is specified then the column has affinity BLOB.
 *   4) If the declared type for a column contains any of the strings "REAL",
 *   "FLOA", or "DOUB" then the column has REAL affinity.
 *   5) Otherwise, the affinity is NUMERIC.
 *   See:  {@link http://stackoverflow.com/a/8417411}
 */
datatype_custom "Custom Datatype Name"
  = t:( name ) r:( datatype_word_tail )* {
    const variant = foldStringKey([ t, r ]);
    let affinity = 'numeric';
    if (/int/i.test(variant)) {
      affinity = 'integer';
    } else if (/char|clob|text/i.test(variant)) {
      affinity = 'text';
    } else if (/blob/i.test(variant)) {
      affinity = 'blob';
    } else if (/real|floa|doub/i.test(variant)) {
      affinity = 'real';
    }
    return {
      'type': 'datatype',
      'variant': variant,
      'affinity': affinity
    };
  }
datatype_word_tail
  = [\t ] w:( name_unquoted ) {
    return w;
  }

type_definition_args "Type Definition Arguments"
  = sym_popen a1:( literal_number_signed ) o a2:( definition_args_loop )? sym_pclose
  {
    return {
      'args': {
        'type': 'expression',
        'variant': 'list',
        'expression': flattenAll([ a1, a2 ])
      }
    };
  }

definition_args_loop
  = sym_comma o n:( literal_number_signed ) o
  { return n; }

/**
 * Literal value definition
 * {@link https://www.sqlite.org/syntax/literal-value.html}
 */
literal_value
  = literal_number_signed
  / literal_number
  / literal_blob
  / literal_null
  / literal_date
  / literal_string

literal_null "Null Literal"
  = n:( NULL ) o
  {
    return {
      'type': 'literal',
      'variant': 'null',
      'value': keyNode(n)
    };
  }

literal_date "Date Literal"
  = d:( CURRENT_DATE / CURRENT_TIMESTAMP / CURRENT_TIME ) o
  {
    return {
      'type': 'literal',
      'variant': 'date',
      'value': keyNode(d)
    };
  }

/**
 * @note
 *   1) [ENFORCED] SQL uses single quotes for string literals.
 *   2) [NOT IMPLEMENTED] Value is an identier or a string literal based on context.
 *   3) [IMPLEMENTED] SQLite allows a negative default value on an added text column.
 *   {@link https://www.sqlite.org/lang_keywords.html}
 */
literal_string "String Literal"
  = n:( number_sign )? s:( literal_string_single )
  {
    return {
      'type': 'literal',
      'variant': 'text',
      'value': s
    };
  }

literal_string_single "Single-quoted String Literal"
  = sym_sglquote s:( literal_string_schar )* sym_sglquote
  {
    /**
     * @note Unescaped the pairs of literal single quotation marks
     */
    /**
     * @note Not sure if the BLOB type should be un-escaped
     */
    return unescape(s, "'");
  }

literal_string_schar
  = "''"
  / [^\']

literal_blob "Blob Literal"
  = [x]i b:( literal_string_single )
  {
    return {
      'type': 'literal',
      'variant': 'blob',
      'value': b
    };
  }

/**
 * @note
 *   This is an undocumented SQLite feature that allows you to specify a
 *   default column value as an _unquoted_ (e.g., DEFAULT foo) or a
 *   double-quoted string value (e.g., DEFAULT "foo").
 */
literal_text
  = n:( name_unquoted / name_dblquoted ) {
    return {
      'type': 'literal',
      'variant': 'text',
      'value': n
    };
  }

number_sign "Number Sign"
  = s:( sym_plus / sym_minus ) { return s; }

literal_number_signed
  = s:( number_sign )? n:( literal_number )
  {
    if (isOkay(s)) {
      n['value'] = foldStringWord([ s, n['value'] ]);
    }
    return n;
  }

literal_number
  = literal_number_hex
  / literal_number_decimal

literal_number_decimal
  = d:( number_decimal_node ) e:( number_decimal_exponent )?
  {
    return {
      'type': 'literal',
      'variant': 'decimal',
      'value': foldStringWord([ d, e ])
    };
  }

number_decimal_node "Decimal Literal"
  = number_decimal_full
  / number_decimal_fraction

number_decimal_full
  = f:( number_digit )+ b:( number_decimal_fraction )?
  { return foldStringWord([ f, b ]); }

number_decimal_fraction
  = t:( sym_dot ) d:( number_digit )*
  { return foldStringWord([ t, d ]); }

number_decimal_exponent "Decimal Literal Exponent"
  = e:( "E"i ) s:( [\+\-] )? d:( number_digit )+
  { return foldStringWord([ e, s, d ]); }

literal_number_hex "Hexidecimal Literal"
  = f:( "0x"i ) b:( number_hex )+
  {
    return {
      'type': 'literal',
      'variant': 'hexidecimal',
      'value': foldStringWord([ f, b ])
    };
  }

number_hex
  = [0-9a-f]i

number_digit
  = [0-9]

/**
 * Bind Parameters have several syntax variations:
 * 1) "?" ( [0-9]+ )?
 * 2) [\$\@\:] name_char+
 * {@link https://www.sqlite.org/c3ref/bind_parameter_name.html}
 */
bind_parameter "Bind Parameter"
  = b:( bind_parameter_numbered / bind_parameter_named / bind_parameter_tcl )
  {
    return Object.assign({
      'type': 'variable'
    }, b);
  }

/**
 * Bind parameters start at index 1 instead of 0.
 */
bind_parameter_numbered "Numbered Bind Parameter"
  = q:( sym_quest ) id:( bind_number_id )? o
  {
    return {
      'format': 'numbered',
      'name': foldStringWord([ q, id ])
    };
  }

bind_number_id
  = f:( [1-9] ) r:( number_digit* ) {
    return foldStringWord([ f, r ]);
  }

bind_parameter_named "Named Bind Parameter"
  = s:( [\:\@] ) name:( name_char )+ o
  {
    return {
      'format': 'named',
      'name': foldStringWord([ s, name ])
    };
  }

bind_parameter_tcl "TCL Bind Parameter"
  = d:( "$" ) name:( name_char / ":" )+ o s:( tcl_suffix )?
  {
    return Object.assign({
      'format': 'tcl',
      'name': foldStringWord([ d, name ])
    }, s);
  }

tcl_suffix
  = sfx:( name_dblquoted ) o
  {
    return {
      'suffix': sfx
    };
  }

/* START: Unary and Binary Expression
 * Syntax: v2.0
 * {@link https://www.sqlite.org/lang_expr.html}
 */

expression_exists "EXISTS Expression"
  = n:( expression_exists_ne )? o e:( select_wrapped )
  {
    if (isOkay(n)) {
      return {
        'type': 'expression',
        'format': 'unary',
        'variant': 'exists',
        'expression': e,
        'operator': keyNode(n)
      };
    }
    return e;
  }
expression_exists_ne "EXISTS Keyword"
  = n:( expression_is_not )? x:( EXISTS ) o
  { return foldStringKey([ n, x ]); }

expression_raise "RAISE Expression"
  = s:( RAISE ) o sym_popen o a:( expression_raise_args ) o sym_pclose
  {
    return Object.assign({
      'type': 'expression',
      'format': 'unary',
      'variant': keyNode(s),
      'expression': a
    }, a);
  }

expression_raise_args "RAISE Expression Arguments"
  = a:( raise_args_ignore / raise_args_message )
  {
    return Object.assign({
      'type': 'error'
    }, a);
  }

raise_args_ignore "IGNORE Keyword"
  = f:( IGNORE )
  {
    return {
      'action': keyNode(f)
    };
  }

raise_args_message
  = f:( ROLLBACK / ABORT / FAIL ) o sym_comma o m:( error_message )
  {
    return {
      'action': keyNode(f),
      'message': m
    };
  }

expression_root
  = bind_parameter
  / function_call
  / literal_value
  / id_column

expression_wrapped
  = sym_popen o n:( expression ) o sym_pclose {
    return n;
  }

expression_recur
  = expression_wrapped
  / expression_exists
  / expression_cast
  / expression_case
  / expression_raise
  / expression_root

expression_unary_collate
  = e:( expression_recur ) o c:( expression_collate ) {
    return Object.assign(c, {
      'expression': e
    });
  }
  / expression_recur

/**
 * @note
 *   Bind to expression_root before expression to bind the unary
 *   operator to the closest expression first.
 */
expression_unary
  = op:( expression_unary_op ) o e:( expression_unary_collate / expression ) {
    return {
      'type': 'expression',
      'format': 'unary',
      'variant': 'operation',
      'expression': e,
      'operator': keyNode(op)
    };
  }
  / expression_unary_collate
expression_unary_op
  = sym_tilde
  / sym_minus
  / sym_plus
  / $( expression_is_not !EXISTS )

expression_collate "COLLATE Expression"
  = c:( column_collate ) {
    return Object.assign({
      'type': 'expression',
      'format': 'unary',
      'variant': 'operation',
      'operator': 'collate'
    }, c);
  }

expression_concat
  = f:( expression_unary ) rest:( o expression_concat_op o expression_unary )*
  { return composeBinary(f, rest); }
expression_concat_op
  = binary_concat

expression_multiply
  = f:( expression_concat ) rest:( o expression_multiply_op o expression_concat )*
  { return composeBinary(f, rest); }
expression_multiply_op
  = binary_multiply
  / binary_divide
  / binary_mod

expression_add
  = f:( expression_multiply ) rest:( o expression_add_op o expression_multiply )*
  { return composeBinary(f, rest); }
expression_add_op
  = binary_plus
  / binary_minus

expression_shift
  = f:( expression_add ) rest:( o expression_shift_op o expression_add )*
  { return composeBinary(f, rest); }
expression_shift_op
  = binary_left
  / binary_right
  / binary_and
  / $(binary_or !binary_or)

expression_compare
  = f:( expression_shift ) rest:( o expression_compare_op o expression_shift )*
  { return composeBinary(f, rest); }
expression_compare_op
  = binary_lte
  / binary_gte
  / $(binary_lt !expression_shift_op)
  / $(binary_gt !expression_shift_op)

expression_equiv
  = f:( expression_compare ) rest:( expression_equiv_tails )*
  { return composeBinary(f, rest); }
expression_equiv_tails
  = o i:( expression_equiv_null_op ) {
    return [null, i, null, {
      'type': 'literal',
      'variant': 'null',
      'value': 'null'
    }];
  }
  / o expression_equiv_op o expression_compare
expression_equiv_null_op
  = "NOT "i o "NULL"i { return 'not'; }
  / ISNULL { return 'is'; }
  / NOTNULL { return 'not'; }
expression_equiv_op
  = binary_lang
  / binary_notequal_a
  / binary_notequal_b
  / binary_equal

expression_cast "CAST Expression"
  = s:( CAST ) o sym_popen e:( expression ) o a:( type_alias ) o sym_pclose
  {
    return {
      'type': 'expression',
      'format': 'unary',
      'variant': keyNode(s),
      'expression': e,
      'as': a
    };
  }
type_alias "Type Alias"
  = AS o d:( type_definition )
  { return d; }

expression_case "CASE Expression"
  = t:( CASE ) o e:( case_expression  )? o w:( expression_case_when )+ o
    s:( expression_case_else )? o END o
  {
    return Object.assign({
      'type': 'expression',
      'variant': keyNode(t),
      'expression': flattenAll([ w, s ])
    }, e);
  }
case_expression
  = !WHEN e:( expression ) {
    return {
      'discriminant': e
    };
  }
expression_case_when "WHEN Clause"
  = s:( WHEN ) o w:( expression ) o THEN o t:( expression ) o
  {
    return {
      'type': 'condition',
      'variant': keyNode(s),
      'condition': w,
      'consequent': t
    };
  }
expression_case_else "ELSE Clause"
  = s:( ELSE ) o e:( expression ) o
  {
    return {
      'type': 'condition',
      'variant': keyNode(s),
      'consequent': e
    };
  }

expression_postfix
  = v:( expression_equiv ) o p:( expression_postfix_tail ) {
    return Object.assign(p, {
      'left': v
    });
  }
  / expression_equiv
expression_postfix_tail
  = expression_in
  / expression_between
  / expression_like

expression_like "Comparison Expression"
  = n:( expression_is_not )?
    m:( LIKE / GLOB / REGEXP / MATCH ) o e:( expression ) o
    x:( expression_escape )? {
    return Object.assign({
      'type': 'expression',
      'format': 'binary',
      'variant': 'operation',
      'operation': foldStringKey([ n, m ]),
      'right': e
    }, x);
  }
expression_escape "ESCAPE Expression"
  = s:( ESCAPE ) o e:( expression ) o
  {
    return {
      'escape': e
    };
  }

expression_between "BETWEEN Expression"
  = n:( expression_is_not )? b:( BETWEEN ) o tail:( expression_between_tail )
  {
    return {
      'type': 'expression',
      'format': 'binary',
      'variant': 'operation',
      'operation': foldStringKey([ n, b ]),
      'right': tail
    };
  }
expression_between_tail
  = f:( expression_postfix ) rest:( o AND o expression_postfix )
  { return composeBinary(f, [ rest ]); }
expression_is_not
  = n:( NOT ) o
  { return keyNode(n); }

expression_in "IN Expression"
  = n:( expression_is_not )? i:( IN ) o e:( expression_in_target )
  {
    return {
      'type': 'expression',
      'format': 'binary',
      'variant': 'operation',
      'operation': foldStringKey([ n, i ]),
      'right': e
    };
  }
expression_in_target
  = expression_list_or_select
  / id_table
expression_list_or_select
  = sym_popen e:( stmt_select_full / expression_list ) o sym_pclose
  { return e; }

expression_and
  = f:( expression_postfix ) rest:( o expression_and_op o expression_postfix )*
  { return composeBinary(f, rest); }
expression_and_op
  = AND

expression
  = f:( expression_and ) rest:( o expression_or_op o expression_and )*
  { return composeBinary(f, rest); }
expression_or_op
  = OR

/* END: Unary and Binary Expression */

expression_list "Expression List"
  = l:( expression_list_loop )? o {
    return {
      'type': 'expression',
      'variant': 'list',
      'expression': isOkay(l) ? l : []
    };
  }
expression_list_loop
  = f:( expression ) o rest:( expression_list_rest )* {
    return flattenAll([ f, rest ]);
  }
expression_list_rest
  = sym_comma e:( expression ) o
  { return e; }

/**
 * @note
 *  Allow functions to have datatype names: date(arg), time(now), etc...
 */
function_call "Function Call"
  = n:( id_function ) o sym_popen a:( function_call_args )? o sym_pclose
  {
    return Object.assign({
      'type': 'function',
      'name': n
    }, a);
  }

function_call_args "Function Call Arguments"
  = s:( select_star ) {
    return {
      'args': {
        'type': 'identifier',
        'variant': 'star',
        'name': s
      }
    };
  }
  / d:( args_list_distinct )? e:( expression_list ) & {
    return !isOkay(d) || e['expression'].length > 0;
  } {
    return {
      'args': Object.assign(e, d)
    };
  }

args_list_distinct
  = s:( DISTINCT / ALL ) o
  {
    return {
      'filter': keyNode(s)
    };
  }

error_message "Error Message"
  = m:( literal_string )
  { return m; }

stmt "Statement"
  = m:( stmt_modifier )? s:( stmt_nodes ) o
  {
    return Object.assign(s, m);
  }

stmt_modifier "QUERY PLAN"
  = e:( EXPLAIN ) o q:( modifier_query )?
  {
    return {
      'explain': isOkay(e)
    };
  }

modifier_query "QUERY PLAN Keyword"
  = q:( QUERY ) o p:( PLAN ) o
  { return foldStringKey([ q, p ]); }

stmt_nodes
  = stmt_crud
  / stmt_create
  / stmt_drop
  / stmt_begin
  / stmt_commit
  / stmt_alter
  / stmt_rollback
  / stmt_savepoint
  / stmt_release
  / stmt_sqlite

/**
 * @note
 *   Transaction statement rules do not follow the transaction nesting rules
 *   for the BEGIN, COMMIT, and ROLLBACK statements.
 *   {@link https://www.sqlite.org/lang_savepoint.html}
 */
stmt_commit "END Transaction Statement"
  = s:( COMMIT / END ) o t:( commit_transaction )?
  {
    return {
      'type': 'statement',
      'variant': 'transaction',
      'action': 'commit'
    };
  }

stmt_begin "BEGIN Transaction Statement"
  = s:( BEGIN ) o m:( stmt_begin_modifier )? t:( commit_transaction )? n:( savepoint_name )?
  {
    return Object.assign({
      'type': 'statement',
      'variant': 'transaction',
      'action': 'begin'
    }, m, n);
  }

commit_transaction
  = t:( TRANSACTION ) o
  { return t; }

stmt_begin_modifier
  = m:( DEFERRED / IMMEDIATE / EXCLUSIVE ) o
  {
    return {
      'defer': keyNode(m)
    };
  }

stmt_rollback "ROLLBACK Statement"
  = s:( ROLLBACK ) o ( commit_transaction )? n:( rollback_savepoint )?
  {
    return Object.assign({
      'type': 'statement',
      'variant': 'transaction',
      'action': 'rollback'
    }, n);
  }

rollback_savepoint "TO Clause"
  = ( TO o )? ( savepoint_alt )? n:( savepoint_name ) {
    return n;
  }

savepoint_name
  = n:( id_savepoint ) o {
    return {
      'savepoint': n
    }
  }

savepoint_alt
  = s:( SAVEPOINT ) o
  { return keyNode(s); }

stmt_savepoint "SAVEPOINT Statement"
  = s:( savepoint_alt ) n:( savepoint_name )
  {
    return {
      'type': 'statement',
      'variant': s,
      'target': n
    };
  }

stmt_release "RELEASE Statement"
  = s:( RELEASE ) o a:( savepoint_alt )? n:( savepoint_name )
  {
    return {
      'type': 'statement',
      'variant': keyNode(s),
      'target': n
    };
  }

stmt_alter "ALTER TABLE Statement"
  = s:( alter_start ) n:( id_table ) o e:( alter_action ) o
  {
    return Object.assign({
      'type': 'statement',
      'variant': keyNode(s),
      'target': n
    }, e);
  }

alter_start "ALTER TABLE Keyword"
  = a:( ALTER ) o t:( TABLE ) o
  { return foldStringKey([ a, t ]); }

alter_action
  = alter_action_rename
  / alter_action_add

alter_action_rename "RENAME TO Keyword"
  = s:( RENAME ) o TO o n:( id_table )
  {
    return {
      'action': keyNode(s),
      'name': n
    };
  }

alter_action_add "ADD COLUMN Keyword"
  = s:( ADD ) o ( action_add_modifier )? d:( source_def_column )
  {
    return {
      'action': keyNode(s),
      'definition': d
    };
  }

action_add_modifier
  = s:( COLUMN ) o
  { return keyNode(s); }

stmt_crud
  = w:( stmt_core_with ) s:( stmt_crud_types )
  { return Object.assign(s, w); }

stmt_core_with "WITH Clause"
  = w:( clause_with )? o
  {
    return w;
  }

clause_with
  = s:( WITH ) o v:( clause_with_recursive )? t:( clause_with_tables )
  {
    var recursive = {
      'variant': isOkay(v) ? 'recursive' : 'common'
    };
    if (isArrayOkay(t)) {
      // Add 'recursive' property into each table expression
      t = t.map(function (elem) {
        return Object.assign(elem, recursive);
      });
    }
    return {
      'with': t
    };
  }

clause_with_recursive
  = s:( RECURSIVE ) o
  { return keyNode(s); }

clause_with_tables
  = f:( expression_cte ) o r:( clause_with_loop )*
  { return flattenAll([ f, r ]); }

clause_with_loop
  = sym_comma e:( expression_cte ) o
  { return e; }

expression_cte "Common Table Expression"
  = t:( id_cte ) s:( select_alias )
  {
    return Object.assign({
      'type': 'expression',
      'format': 'table',
      'variant': 'common',
      'target': t
    }, s);
  }

select_alias
  = AS o s:( select_wrapped )
  {
    return {
      'expression': s
    };
  }

select_wrapped
  = sym_popen s:( stmt_select_full ) o sym_pclose {
    return s;
  }

stmt_select_full
  = w:( stmt_core_with ) s:( stmt_select ) {
    return Object.assign(s, w);
  }

/**
 * @note Uncommon or SQLite-specific statement types
 */
stmt_sqlite
  = stmt_attach
  / stmt_detach
  / stmt_vacuum
  / stmt_analyze
  / stmt_reindex
  / stmt_pragma

stmt_attach "ATTACH Statement"
  = a:( ATTACH ) o b:( DATABASE o )? e:( expression ) o AS o n:( attach_arg ) o
  {
    return {
      'type': 'statement',
      'variant': keyNode(a),
      'target': n,
      'attach': e
    };
  }

attach_arg
  = id_database / literal_null / bind_parameter

stmt_detach "DETACH Statement"
  = d:( DETACH ) o b:( DATABASE o )? n:( attach_arg ) o
  {
    return {
      'type': 'statement',
      'variant': keyNode(d),
      'target': n
    };
  }

/**
 * @note
 *   Specifying a target after the VACUUM statement appears to be an
 *   undocumented feature.
 */
stmt_vacuum "VACUUM Statement"
  = v:( VACUUM ) o t:( vacuum_target )?
  {
    return Object.assign({
      'type': 'statement',
      'variant': 'vacuum'
    }, t);
  }
vacuum_target
  = t:( id_database ) o {
    return {
      'target': t
    };
  }

/**
 * @note
 *   The argument from this statement cannot be categorized as a
 *   table or index based on context, so only the name is included.
 */
stmt_analyze "ANALYZE Statement"
  = s:( ANALYZE ) o a:( analyze_arg )?
  {
    return Object.assign({
      'type': 'statement',
      'variant': keyNode(s)
    }, a);
  }

analyze_arg
  = n:( id_table / id_index / id_database ) o
  {
    return {
      'target': n['name']
    };
  }

/**
 * @note
 *   The argument from this statement cannot be categorized as a
 *   table or index based on context, so only the name is included.
 */
stmt_reindex "REINDEX Statement"
  = s:( REINDEX ) o a:( reindex_arg )? o
  {
    return Object.assign({
      'type': 'statement',
      'variant': keyNode(s)
    }, a);
  }

reindex_arg
  = a:( id_table / id_index / id_collation ) o
  {
    return {
      'target': a['name']
    };
  }

stmt_pragma "PRAGMA Statement"
  = s:( PRAGMA ) o n:( id_pragma ) o v:( pragma_expression )?
  {
    return {
      'type': 'statement',
      'variant': keyNode(s),
      'target': n,
      'args': {
        'type': 'expression',
        'variant': 'list',
        'expression': v
      }
    };
  }

pragma_expression
  = sym_popen v:( pragma_value ) o sym_pclose { return v; }
  / sym_equal v:( pragma_value ) o { return v; }

pragma_value
  = pragma_value_bool
  / pragma_value_literal
  / pragma_value_name

/**
 * @note
 *   Allow double quoted string literals as pragma values but do not treat
 *   them as an identifier because it is impossible from the surrounding
 *   context to determine whether it is a column, table, or database
 *   identifier.
 */
pragma_value_literal
  = literal_number_signed
  / literal_string
  / literal_text

/**
 * @note
 *   There is no such thing as a boolean literal in SQLite
 *   {@link http://www.sqlite.org/datatype3.html}. However, the
 *   documentation for PRAGMA mentions the ability to use
 *   literal boolean values in this one specific instance.
 *   See: {@link https://www.sqlite.org/pragma.html}
 */
pragma_value_bool
  = v:( pragma_bool_id ) & { return /^(yes|no|on|off|false|true|0|1)$/i.test(v) }
  {
    return {
      'type': 'literal',
      'variant': 'boolean',
      'normalized': (/^(yes|on|true|1)$/i.test(v) ? '1' : '0'),
      'value': v
    };
  }
pragma_bool_id
  = n:( name_char )+ {
    return keyNode(n);
  }

pragma_value_name
  = n:( pragma_bool_id )
  {
    return {
      'type': 'identifier',
      'variant': 'name',
      'name': n
    };
  }

stmt_crud_types
  = stmt_select
  / stmt_insert
  / stmt_update
  / stmt_delete

/** {@link https://www.sqlite.org/lang_select.html} */
stmt_select "SELECT Statement"
  = s:( select_loop ) o o:( stmt_core_order )? o l:( stmt_core_limit )?
  {
    return Object.assign(s, o, l);
  }

stmt_core_order "ORDER BY Clause"
  = ORDER o BY o d:( stmt_core_order_list )
  {
    return {
      'order': d['result']
    };
  }

stmt_core_limit "LIMIT Clause"
  = s:( LIMIT ) o e:( expression ) o d:( stmt_core_limit_offset )?
  {
    return {
      'limit': Object.assign({
        'type': 'expression',
        'variant': 'limit',
        'start': e
      }, d)
    };
  }

stmt_core_limit_offset "OFFSET Clause"
  = o:( limit_offset_variant ) e:( expression )
  {
    return {
      'offset': e
    };
  }

limit_offset_variant
  = limit_offset_variant_name
  / sym_comma

limit_offset_variant_name
  = s:( OFFSET ) o
  { return keyNode(s); }

select_loop
  = s:( select_parts ) o u:( select_loop_union )*
  {
    if (isArrayOkay(u)) {
      return {
        'type': 'statement',
        'variant': 'compound',
        'statement': s,
        'compound': u
      };
    } else {
      return s;
    }
  }

select_loop_union "Union Operation"
  = c:( operator_compound ) o s:( select_parts ) o
  {
    return {
      'type': 'compound',
      'variant': c,
      'statement': s
    };
  }

select_parts
  = select_parts_core
  / select_parts_values

select_parts_core
  = s:( select_core_select ) f:( select_core_from )? w:( stmt_core_where )?
    g:( select_core_group )?
  {
    return Object.assign({
      'type': 'statement',
      'variant': 'select',
    }, s, f, w, g);
  }

select_core_select "SELECT Results Clause"
  = SELECT o d:( select_modifier )? o t:( select_target )
  {
    return Object.assign({
      'result': t
    }, d);
  }

select_modifier "SELECT Results Modifier"
  = select_modifier_distinct
  / select_modifier_all

select_modifier_distinct
  = s:( DISTINCT ) o
  {
    return {
      'distinct': true
    };
  }

select_modifier_all
  = s:( ALL ) o
  {
    return {};
  }

select_target
  = f:( select_node ) o r:( select_target_loop )*
  { return flattenAll([ f, r ]); }

select_target_loop
  = sym_comma n:( select_node ) o
  { return n; }

select_core_from "FROM Clause"
  = f:( FROM ) o s:( select_source ) o
  {
    return {
      'from': s
    };
  }

stmt_core_where "WHERE Clause"
  = f:( WHERE ) o e:( expression ) o
  {
    return {
      'where': makeArray(e)
    };
  }

select_core_group "GROUP BY Clause"
  = f:( GROUP ) o BY o e:( expression_list ) o h:( select_core_having )?
  {
    return Object.assign({
      'group': e
    }, h);
  }

select_core_having "HAVING Clause"
  = f:( HAVING ) o e:( expression ) o
  {
    return {
      'having': e
    };
  }

select_node
  = select_node_star
  / select_node_aliased

select_node_star
  = q:( select_node_star_qualified )? s:( select_star )
  {
    return {
      'type': 'identifier',
      'variant': 'star',
      'name': foldStringWord([ q, s ])
    };
  }

select_node_star_qualified
  = n:( name ) s:( sym_dot )
  { return foldStringWord([ n, s ]); }

select_node_aliased
  = e:( expression ) o a:( alias )?
  {
    return Object.assign(e, a);
  }

select_source
  = f:( table_or_sub ) o t:( source_loop_tail )*
  {
    if (isArrayOkay(t)) {
      return {
        'type': 'map',
        'variant': 'join',
        'source': f,
        'map': t
      };
    }
    return f;
  }

source_loop_tail
  = cl:( select_cross_clause / select_join_clause ) c:( join_condition )? {
    return Object.assign(cl, c);
  }

select_cross_clause "CROSS JOIN Operation"
  = sym_comma n:( table_or_sub ) o {
    return {
      'type': 'join',
      'variant': 'cross join',
      'source': n
    };
  }

select_join_clause "JOIN Operation"
  = o:( join_operator ) o n:( table_or_sub ) o
  {
    return {
      'type': 'join',
      'variant': keyNode(o),
      'source': n
    };
  }

table_or_sub
  = table_or_sub_sub
  / bind_parameter
  / table_or_sub_func
  / table_qualified
  / table_or_sub_select

table_or_sub_func
  = n:( id_function ) o l:( expression_list_wrapped ) o a:( alias )? {
    return Object.assign({
      'type': 'function',
      'variant': 'table',
      'name': n,
      'args': l
    }, a);
  }

table_qualified "Qualified Table"
  = d:( table_qualified_id ) o i:( table_or_sub_index_node )?
  {
    return Object.assign(d, i);
  }

table_qualified_id "Qualified Table Identifier"
  = n:( id_table ) o a:( alias )?
  {
    return Object.assign(n, a);
  }


table_or_sub_index_node "Qualfied Table Index"
  = index_node_indexed
  / index_node_none

index_node_indexed
  = s:( INDEXED ) o BY o n:( id_index ) o
  {
    return {
      'index': n
    };
  }

index_node_none
  = n:( expression_is_not ) i:( INDEXED ) o
  {
    // TODO: Not sure what should happen here
    return {
      'index': foldStringKey([ n, i ])
    };
  }

table_or_sub_sub "SELECT Source"
  = sym_popen l:( select_source ) o sym_pclose a:( alias )?
  { return Object.assign(l, a); }

table_or_sub_select "Subquery"
  = s:( select_wrapped ) a:( alias )?
  {
    return Object.assign(s, a);
  }

alias "Alias"
  = a:( AS ( !( name_char / reserved_critical_list ) o ) )? n:( name ) o
  {
    return {
      'alias': n
    };
  }

join_operator "JOIN Operator"
  = n:( join_operator_natural )? o t:( join_operator_types )? j:( JOIN )
  { return foldStringKey([ n, t, j ]); }

join_operator_natural
  = n:( NATURAL ) o
  { return keyNode(n); }

join_operator_types
  = operator_types_hand
  / operator_types_misc

/**
 * @note
 *   FULL (OUTER)? JOIN included from PostgreSQL although it is not a
 *   join operarator allowed in SQLite.
 *   See: {@link https://www.sqlite.org/syntax/join-operator.html}
 */
operator_types_hand
  = t:( LEFT / RIGHT / FULL ) o o:( types_hand_outer )?
  { return foldStringKey([ t, o ]); }

types_hand_outer
  = t:( OUTER ) o
  { return keyNode(t); }

operator_types_misc
  = t:( INNER / CROSS ) o
  { return keyNode(t); }

join_condition "JOIN Constraint"
  = c:( join_condition_on / join_condition_using ) o
  {
    return {
      'constraint': Object.assign({
        'type': 'constraint',
        'variant': 'join'
      }, c)
    }
  }

join_condition_on "Join ON Clause"
  = s:( ON ) o e:( expression )
  {
    return {
      'format': keyNode(s),
      'on': e
    };
  }

join_condition_using "Join USING Clause"
  = s:( USING ) o e:( loop_columns )
  {
    return {
      'format': keyNode(s),
      'using': e
    };
  }


select_parts_values "VALUES Clause"
  = s:( VALUES ) o l:( insert_values_list )
  {
    return {
      'type': 'statement',
      'variant': 'select',
      'result': l
    };
  }

stmt_core_order_list
  = f:( stmt_core_order_list_item ) o b:( stmt_core_order_list_loop )*
  {
    return {
      'result': flattenAll([ f, b ])
    };
  }

stmt_core_order_list_loop
  = sym_comma i:( stmt_core_order_list_item ) o
  { return i; }

stmt_core_order_list_item "Ordering Expression"
  = e:( expression ) o d:( primary_column_dir )?
  {
    // Only convert this into an ordering expression if it contains
    // more than just the expression.
    if (isOkay(d)) {
      return Object.assign({
        'type': 'expression',
        'variant': 'order',
        'expression': e
      }, d);
    }
    return e;
  }

select_star "Star"
  = sym_star

stmt_fallback_types "Fallback Type"
  = REPLACE
  / ROLLBACK
  / ABORT
  / FAIL
  / IGNORE

/** {@link https://www.sqlite.org/lang_insert.html} */
stmt_insert "INSERT Statement"
  = k:( insert_keyword ) o t:( insert_target )
  {
    return Object.assign({
      'type': 'statement',
      'variant': 'insert'
    }, k, t);
  }

insert_keyword
  = insert_keyword_ins
  / insert_keyword_repl

insert_keyword_ins "INSERT Keyword"
  = a:( INSERT ) o m:( insert_keyword_mod )?
  {
    return Object.assign({
      'action': keyNode(a)
    }, m);
  }

insert_keyword_repl "REPLACE Keyword"
  = a:( REPLACE ) o
  {
    return {
      'action': keyNode(a)
    };
  }

insert_keyword_mod "INSERT OR Modifier"
  = s:( OR ) o m:( stmt_fallback_types )
  {
    return {
      'or': keyNode(m)
    };
  }

insert_target
  = i:( insert_into ) r:( insert_results )
  {
    return Object.assign({
      'into': i
    }, r);
  }

insert_into "INTO Clause"
  = s:( insert_into_start ) t:( id_cte )
  {
    return t;
  }

insert_into_start "INTO Keyword"
  = s:( INTO ) o

insert_results "VALUES Clause"
  = r:( insert_value / stmt_select_full / insert_default ) o
  {
    return {
      'result': r
    };
  }

loop_columns "Column List"
  = sym_popen f:( loop_name ) o b:( loop_column_tail )* sym_pclose
  {
    return {
      'columns': flattenAll([ f, b ])
    };
  }

loop_column_tail
  = sym_comma c:( loop_name ) o
  { return c; }

loop_name "Column Name"
  = n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'column',
      'name': n
    };
  }

insert_value "VALUES Clause"
  = s:( insert_value_start ) r:( insert_values_list )
  { return r; }

insert_value_start "VALUES Keyword"
  = s:( VALUES ) o
  { return keyNode(s); }

insert_values_list
  = f:( expression_list_wrapped ) o b:( insert_values_loop )*
  { return flattenAll([ f, b ]); }

insert_values_loop
  = sym_comma e:( expression_list_wrapped ) o
  { return e; }

expression_list_wrapped "Wrapped Expression List"
  = sym_popen e:( expression_list ) o sym_pclose {
    return e;
  }

insert_default "DEFAULT VALUES Clause"
  = d:( DEFAULT ) o v:( VALUES )
  {
    return {
      'type': 'values',
      'variant': 'default'
      // TODO: Not sure what should go here
      // , 'values': null
    };
  }

operator_compound "Compound Operator"
  = s:( compound_union / INTERSECT / EXCEPT )
  { return keyNode(s); }

compound_union "UNION Operator"
  = s:( UNION ) o a:( compound_union_all )?
  { return foldStringKey([ s, a ]); }

compound_union_all
  = a:( ALL ) o
  { return a; }

/**
 * @note
 *   Includes limited update syntax
 *   {@link https://www.sqlite.org/syntax/update-stmt-limited.html}
 */
stmt_update "UPDATE Statement"
  = s:( update_start ) f:( update_fallback )?
    t:( table_qualified ) o u:( update_set ) w:( stmt_core_where )?
    o:( stmt_core_order )? o l:( stmt_core_limit )?
  {
    return Object.assign({
      'type': 'statement',
      'variant': s,
      'into': t
    }, f, u, w, o, l);
  }

update_start "UPDATE Keyword"
  = s:( UPDATE ) o
  { return keyNode(s); }

update_fallback "UPDATE OR Modifier"
  = OR o t:( stmt_fallback_types ) o
  {
    return {
      'or': keyNode(t)
    };
  }

update_set "SET Clause"
  = SET o c:( update_columns ) o
  {
    return {
      'set': c
    };
  }

update_columns
  = f:( update_column ) b:( update_columns_tail )*
  { return flattenAll([ f, b ]); }

update_columns_tail
  = o sym_comma c:( update_column )
  { return c; }

update_column "Column Assignment"
  = f:( id_column ) o sym_equal e:( expression ) o
  {
    return {
      'type': 'assignment',
      'target': f,
      'value': e
    };
  }

/**
 * @note
 *   Includes limited update syntax
 *   {@link https://www.sqlite.org/syntax/delete-stmt-limited.html}
 */
stmt_delete "DELETE Statement"
  = s:( delete_start ) t:( table_qualified ) o w:( stmt_core_where )?
    o:( stmt_core_order )? l:( stmt_core_limit )?
  {
    return Object.assign({
      'type': 'statement',
      'variant': s,
      'from': t
    }, w, o, l);
  }

delete_start "DELETE Keyword"
  = s:( DELETE ) o FROM o
  { return keyNode(s); }

/**
 * @note
 *   The "only" rules were created to help the tracer to not traverse
 *   the wrong path.
 */
stmt_create "CREATE Statement"
  = create_table_only
  / create_index_only
  / create_trigger_only
  / create_view_only
  / create_virtual_only

create_start
  = s:( CREATE ) o
  { return keyNode(s); }

create_table_only
  = !( create_start ( INDEX / TRIGGER / VIEW / VIRTUAL ) ) c:( create_table )
  { return c; }

create_index_only
  = !( create_start ( TABLE / TRIGGER / VIEW / VIRTUAL ) ) c:( create_index )
  { return c; }

create_trigger_only
  = !( create_start ( TABLE / INDEX / VIEW / VIRTUAL ) ) c:( create_trigger )
  { return c; }

create_view_only
  = !( create_start ( TABLE / INDEX / TRIGGER / VIRTUAL ) ) c:( create_view )
  { return c; }

create_virtual_only
  = !( create_start ( TABLE / INDEX / TRIGGER / VIEW ) ) c:( create_virtual )
  { return c; }

create_table "CREATE TABLE Statement"
  = s:( create_table_start ) ne:( create_core_ine )? id:( id_table ) o
    r:( create_table_source )
  {
    return Object.assign({
      'type': 'statement',
      'name': id
    }, s, r, ne);
  }

create_table_start
  = s:( create_start ) tmp:( create_core_tmp )? t:( TABLE ) o
  {
    return Object.assign({
      'variant': s,
      'format': keyNode(t)
    }, tmp);
  }

create_core_tmp
  = t:( TEMPORARY / TEMP ) o
  {
    return {
      'temporary': isOkay(t)
    };
  }

create_core_ine "IF NOT EXISTS Modifier"
  = i:( IF ) o n:( expression_is_not ) e:( EXISTS ) o
  {
    return {
      'condition': makeArray({
        'type': 'condition',
        'variant': keyNode(i),
        'condition': {
          'type': 'expression',
          'variant': keyNode(e),
          'operator': foldStringKey([ n, e ])
        }
      })
    };
  }

create_table_source
  = table_source_def
  / table_source_select

table_source_def "Table Definition"
  = sym_popen s:( source_def_loop ) t:( source_tbl_loop )* sym_pclose r:( source_def_rowid )?
  {
    return Object.assign({
      'definition': flattenAll([ s, t ])
    }, r);
  }

source_def_rowid
  = r:( WITHOUT ) o w:( ROWID ) o
  {
    return {
      'optimization': [{
        'type': 'optimization',
        'value': foldStringKey([ r, w ])
      }]
    };
  }

source_def_loop
  = f:( source_def_column ) o b:( source_def_tail )*
  { return flattenAll([ f, b ]); }

source_def_tail
  = sym_comma t:( source_def_column ) o
  { return t; }

/**
 * @note
 *  Table constraints should be separated by commas, but they do not have
 *  to be according to SQLite.
 */
source_tbl_loop
  = sym_comma? f:( table_constraint )
  { return f; }

/** {@link https://www.sqlite.org/syntaxdiagrams.html#column-def} */
source_def_column "Column Definition"
  = n:( source_def_name ) o t:( column_type )? c:( column_constraints )?
  {
    return Object.assign({
      'type': 'definition',
      'variant': 'column',
      'name': n,
      'definition': (isOkay(c) ? c : []),
    }, t);
  }
source_def_name
  = n:( name ) &( o ) {
    return n;
  }
  / !( column_type / column_constraint / table_constraint ) o n:( name_reserved ) {
    return n;
  }

column_type "Column Datatype"
  = t:( type_definition ) o
  {
    return {
      'datatype': t
    };
  }

column_constraints
  = f:( column_constraint ) b:( column_constraint_tail )* o
  { return flattenAll([ f, b ]); }

column_constraint_tail
  = o c:( column_constraint )
  { return c; }

/**
 * @note
 *   From SQLite official tests:
 *     Undocumented behavior:  The CONSTRAINT name clause can follow a constraint.
 *     Such a clause is ignored.  But the parser must accept it for backwards
 *     compatibility.
 */
 /** {@link https://www.sqlite.org/syntax/column-constraint.html} */
column_constraint "Column Constraint"
  = n:( constraint_name )? c:( column_constraint_types ) ln:( constraint_name )?
  {
    return Object.assign(c, n);
  }

// Note: Allow an arbitrary number of CONSTRAINT names but take the last
//       one specified in the sequence
constraint_name
  = cl:( constraint_name_loop )+ {
    return cl[cl.length - 1];
  }

constraint_name_loop "CONSTRAINT Name"
  = CONSTRAINT o n:( name ) o {
    return {
      'name': n
    };
  }

column_constraint_types
  = column_constraint_primary
  / column_constraint_null
  / column_constraint_check
  / column_constraint_default
  / column_constraint_collate
  / column_constraint_foreign

column_constraint_foreign "FOREIGN KEY Column Constraint"
  = f:( foreign_clause )
  {
    return Object.assign({
      'variant': 'foreign key'
    }, f);
  }

column_constraint_primary "PRIMARY KEY Column Constraint"
  = p:( col_primary_start ) d:( primary_column_dir )? c:( primary_conflict )?
    a:( col_primary_auto )?
  {
    return Object.assign(p, c, d, a);
  }

/**
 * @note
 *    PRAGMA KEY appears to be another undocumented feature
 *    that is accepted by the SQLite parser but is not documented.
 */
col_primary_start "PRIMARY KEY Keyword"
  = s:( PRIMARY / PRAGMA ) o k:( KEY ) o
  {
    return {
      'type': 'constraint',
      'variant': foldStringKey([ s, k ])
    };
  }

col_primary_auto "AUTOINCREMENT Keyword"
  = a:( AUTOINCREMENT ) o
  {
    return {
      'autoIncrement': true
    };
  }

column_constraint_null
  = s:( constraint_null_types ) c:( primary_conflict )? o
  {
    return Object.assign({
      'type': 'constraint',
      'variant': s
    }, c);
  }

constraint_null_types "UNIQUE Column Constraint"
  = t:( constraint_null_value / UNIQUE ) o
  { return keyNode(t); }

constraint_null_value "NULL Column Constraint"
  = n:( expression_is_not )? l:( NULL )
  { return foldStringKey([ n, l ]); }

column_constraint_check "CHECK Column Constraint"
  = constraint_check

column_constraint_default "DEFAULT Column Constraint"
  = s:( DEFAULT ) o v:( column_default_values ) o
  {
    return {
      'type': 'constraint',
      'variant': keyNode(s),
      'value': v
    };
  }
column_default_values
  = expression_wrapped
  / literal_number_signed
  / literal_value
  / literal_text

column_constraint_collate "COLLATE Column Constraint"
  = c:( column_collate )
  {
    return {
      'type': 'constraint',
      'variant': 'collate',
      'collate': c
    };
  }

/** {@link https://www.sqlite.org/syntax/table-constraint.html} */
/* Note from SQLite official tests:
 * Undocumented behavior:  The CONSTRAINT name clause can follow a constraint.
 * Such a clause is ignored.  But the parser must accept it for backwards
 * compatibility.
 */
table_constraint "Table Constraint"
  = n:( constraint_name )? c:( table_constraint_types ) o nl:( constraint_name )?
  {
    return Object.assign({
      'type': 'definition',
      'variant': 'constraint'
    }, c, n);
  }

table_constraint_types
  = table_constraint_foreign
  / table_constraint_primary
  / table_constraint_check

table_constraint_check "CHECK Table Constraint"
  = c:( constraint_check )
  {
    return {
      'definition': makeArray(c)
    };
  }

table_constraint_primary "PRIMARY KEY Table Constraint"
  = k:( primary_start ) o c:( primary_columns_table ) t:( primary_conflict )?
  {
    return {
      'definition': makeArray(Object.assign(k, t, c[1])),
      'columns': c[0]
    };
  }

primary_start
  = s:( primary_start_normal / primary_start_unique ) o
  {
    return {
      'type': 'constraint',
      'variant': keyNode(s)
    };
  }

primary_start_normal "PRIMARY KEY Keyword"
  = p:( PRIMARY ) o k:( KEY )
  { return foldStringKey([ p, k ]); }

primary_start_unique "UNIQUE Keyword"
  = u:( UNIQUE )
  { return keyNode(u); }

primary_columns
  = sym_popen f:( primary_column ) o b:( primary_column_tail )* sym_pclose {
    return [f].concat(b);
  }
primary_columns_index
  = c:( primary_columns ) {
    return c.map(([ res ]) => res);
  }
primary_columns_table
  = c:( primary_columns ) {
    const auto = c.find(([ res, a ]) => isOkay(a));
    return [
      c.map(([ res, a ]) => res),
      auto ? auto[1] : null
    ];
  }

primary_column_tail
  = sym_comma c:( primary_column ) o
  { return c; }

primary_column "Indexed Column"
  = e:( primary_column_types ) o d:( primary_column_dir )? a:( col_primary_auto )?
  {
    // Only convert this into an ordering expression if it contains
    // more than just the expression.
    let res = e;
    if (isOkay(d)) {
      res = Object.assign({
        'type': 'expression',
        'variant': 'order',
        'expression': e
      }, d);
    }
    return [ res, a ];
  }
primary_column_types
  = n:( loop_name ) &( o ( sym_semi / sym_pclose / primary_column_dir ) ) {
    return n;
  }
  / expression

column_collate "Collation"
  = c: ( column_collate_loop )+ {
    return {
      'collate': makeArray(c)
    };
  }

column_collate_loop
  = COLLATE o n:( id_collation ) o
  {
    return n;
  }

primary_column_dir "Column Direction"
  = t:( ASC / DESC ) o
  {
    return {
      'direction': keyNode(t),
    };
  }

primary_conflict
  = s:( primary_conflict_start ) t:( stmt_fallback_types ) o
  {
    return {
      'conflict': keyNode(t)
    };
  }

primary_conflict_start "ON CONFLICT Keyword"
  = o:( ON ) o c:( CONFLICT ) o
  { return foldStringKey([ o, c ]); }

constraint_check
  = k:( CHECK ) o c:( expression_wrapped )
  {
    return {
      'type': 'constraint',
      'variant': keyNode(k),
      'expression': c
    };
  }

table_constraint_foreign "FOREIGN KEY Table Constraint"
  = k:( foreign_start ) l:( loop_columns ) c:( foreign_clause ) o
  {
    return Object.assign({
      'definition': makeArray(Object.assign(k, c))
    }, l);
  }

foreign_start "FOREIGN KEY Keyword"
  = f:( FOREIGN ) o k:( KEY ) o
  {
    return {
      'type': 'constraint',
      'variant': foldStringKey([ f, k ])
    };
  }

/** {@link https://www.sqlite.org/syntax/foreign-key-clause.html} */
foreign_clause
  = r:( foreign_references ) a:( foreign_actions )? d:( foreign_deferrable )?
  {
    return Object.assign({
      'type': 'constraint'
    }, r, a, d);
  }

foreign_references "REFERENCES Clause"
  = s:( REFERENCES ) o t:( id_cte ) o
  {
    return {
      'references': t
    };
  }

foreign_actions
  = f:( foreign_action ) o b:( foreign_actions_tail )* {
    return {
      'action': flattenAll([ f, b ])
    };
  }

foreign_actions_tail
  = a:( foreign_action ) o
  { return a; }

foreign_action "FOREIGN KEY Action Clause"
  = foreign_action_on
  / foreign_action_match

foreign_action_on
  = m:( ON ) o a:( DELETE / UPDATE ) o n:( action_on_action )
  {
    return {
      'type': 'action',
      'variant': keyNode(m),
      'action': keyNode(n)
    };
  }

action_on_action "FOREIGN KEY Action"
  = on_action_set
  / on_action_cascade
  / on_action_none

on_action_set
  = s:( SET ) o v:( NULL / DEFAULT ) o
  { return foldStringKey([ s, v ]); }

on_action_cascade
  = c:( CASCADE / RESTRICT ) o
  { return keyNode(c); }

on_action_none
  = n:( NO ) o a:( ACTION ) o
  { return foldStringKey([ n, a ]); }

/**
 * @note Not sure what kind of name this should be.
 */
foreign_action_match
  = m:( MATCH ) o n:( name ) o
  {
    return {
      'type': 'action',
      'variant': keyNode(m),
      'action': n
    };
  }

foreign_deferrable "DEFERRABLE Clause"
  = n:( expression_is_not )? d:( DEFERRABLE ) o i:( deferrable_initially )? {
    return {
      'defer': foldStringKey([ n, d, i ])
    };
  }

deferrable_initially
  = i:( INITIALLY ) o d:( DEFERRED / IMMEDIATE ) o
  { return foldStringKey([ i, d ]); }

table_source_select
  = s:( create_as_select )
  {
    return {
      'definition': makeArray(s)
    };
  }

create_index "CREATE INDEX Statement"
  = s:( create_index_start ) ne:( create_core_ine )? n:( id_index ) o
    o:( index_on ) w:( stmt_core_where )?
  {
    return Object.assign({
      'type': 'statement',
      'target': n,
      'on': o,
    }, s, ne, w);
  }

create_index_start
  = s:( create_start ) u:( index_unique )? i:( INDEX ) o
  {
    return Object.assign({
      'variant': keyNode(s),
      'format': keyNode(i)
    }, u);
  }

index_unique
  = u:( UNIQUE ) o
  {
    return {
      'unique': true
    };
  }

index_on "ON Clause"
  = o:( ON ) o t:( id_table ) o c:( primary_columns_index )
  {
    return {
      'type': 'identifier',
      'variant': 'expression',
      'format': 'table',
      'name': t['name'],
      'columns': c
    };
  }

/**
 * @note
 *   This statement type has missing syntax restrictions that need to be
 *   enforced on UPDATE, DELETE, and INSERT statements in the trigger_action.
 *   See {@link https://www.sqlite.org/lang_createtrigger.html}.
 */
 /**
  * @note
  *   Omitting the trigger name in a CREATE TRIGGER is another undocumented
  *   feature of the SQLite parser.
  */
create_trigger "CREATE TRIGGER Statement"
  = s:( create_trigger_start ) ne:( create_core_ine )? n:( id_trigger )? o
    cd:( trigger_conditions ) ( ON ) o o:( id_table ) o
    me:( trigger_foreach )? wh:( trigger_when )? a:( trigger_action )
  {
    return Object.assign({
      'type': 'statement',
      'target': n,
      'on': o,
      'event': cd,
      'by': (isOkay(me) ? me : 'row'),
      'action': makeArray(a)
    }, s, ne, wh);
  }

create_trigger_start
  = s:( create_start ) tmp:( create_core_tmp )? t:( TRIGGER ) o
  {
    return Object.assign({
      'variant': keyNode(s),
      'format': keyNode(t)
    }, tmp);
  }

trigger_conditions "Conditional Clause"
  = m:( trigger_apply_mods )? d:( trigger_do )
  {
    return Object.assign({
      'type': 'event'
    }, m, d);
  }

trigger_apply_mods
  = m:( BEFORE / AFTER / trigger_apply_instead ) o
  {
    return {
      'occurs': keyNode(m)
    };
  }

trigger_apply_instead
  = i:( INSTEAD ) o o:( OF )
  { return foldStringKey([ i, o ]); }

trigger_do "Conditional Action"
  = trigger_do_on
  / trigger_do_update

trigger_do_on
  = o:( DELETE / INSERT ) o
  {
    return {
      'event': keyNode(o)
    };
  }

trigger_do_update
  = s:( UPDATE ) o f:( do_update_of )?
  {
    return {
      'event': keyNode(s),
      'of': f
    };
  }

do_update_of
  = s:( OF ) o c:( do_update_columns )
  { return c; }

do_update_columns
  = f:( loop_name ) o b:( loop_column_tail )*
  { return flattenAll([ f, b ]); }

/**
 *  @note
 *    FOR EACH STATEMENT is not supported by SQLite, but still included here.
 *    See {@link https://www.sqlite.org/lang_createtrigger.html}.
 */
trigger_foreach
  = f:( FOR ) o e:( EACH ) o r:( ROW / "STATEMENT"i ) o
  { return keyNode(r); }

trigger_when "WHEN Clause"
  = w:( WHEN ) o e:( expression ) o {
    return {
      'when': e
    };
  }

trigger_action "Actions Clause"
  = s:( BEGIN ) o a:( action_loop ) o e:( END ) o
  { return a; }

action_loop
  = l:( action_loop_stmt )+
  { return l; }

action_loop_stmt
  = s:( stmt_crud ) o semi_required
  { return s; }

create_view "CREATE VIEW Statement"
  = s:( create_view_start ) ne:( create_core_ine )? n:( id_view_expression ) o
    r:( create_as_select )
  {
    return Object.assign({
      'type': 'statement',
      'target': n,
      'result': r
    }, s, ne);
  }

id_view_expression
  = n:( id_view ) o a:( loop_columns ) {
    return Object.assign({
      'type': 'identifier',
      'variant': 'expression',
      'format': 'view',
      'name': n['name'],
      'columns': []
    }, a);
  }
  / id_view

create_view_start
  = s:( create_start ) tmp:( create_core_tmp )? v:( VIEW ) o
  {
    return Object.assign({
      'variant': keyNode(s),
      'format': keyNode(v)
    }, tmp);
  }

create_as_select
  = s:( AS ) o r:( stmt_select ) o
  { return r; }

create_virtual "CREATE VIRTUAL TABLE Statement"
  = s:( create_virtual_start ) ne:( create_core_ine )? n:( id_table ) o
    ( USING ) o m:( virtual_module )
  {
    return Object.assign({
      'type': 'statement',
      'target': n,
      'result': m
    }, s, ne);
  }

create_virtual_start
  = s:( create_start ) v:( VIRTUAL ) o t:( TABLE ) o
  {
    return {
      'variant': keyNode(s),
      'format': keyNode(v)
    };
  }

virtual_module
  = m:( name_unquoted ) o a:( virtual_args )?
  {
    return Object.assign({
      'type': 'module',
      'variant': 'virtual',
      'name': m
    }, a);
  }

virtual_args "Module Arguments"
  = sym_popen o l:( virtual_args_loop )? o sym_pclose o
  {
    return {
      'args': {
        'type': 'expression',
        'variant': 'list',
        'expression': isOkay(l) ? l : []
      }
    };
  }
/**
 * @note
 *   The offical SQLite parser allows trailing commas in VIRTUAL TABLE
 *   definitions.
 */
virtual_args_loop
  = f:( virtual_arg_types ) b:( virtual_args_tail )* {
    return flattenAll([ f, b ]).filter((arg) => isOkay(arg));
  }
virtual_args_tail
  = o sym_comma o a:( virtual_arg_types )? {
    return a;
  }

virtual_arg_types
  = !( name o ( type_definition / column_constraint ) ) e:( expression ) o {
    return e;
  }
  / n:( virtual_column_name ) ( !( name_char ) o ) t:( column_type )? c:( column_constraints )? {
    return Object.assign({
      'type': 'definition',
      'variant': 'column',
      'name': n,
      'definition': (isOkay(c) ? c : []),
    }, t);
  }

/**
 * @note
 *   SQLite allows reserved words in the a VIRTUAL TABLE statement USING
 *   clause CTE columns (e.g., from, to).
 */
virtual_column_name
  = name
  / name_reserved

stmt_drop "DROP Statement"
  = s:( drop_start ) q:( id_table ) o
  {
    /**
     * @note Manually copy in the correct variant for the target
     */
    return Object.assign({
      'type': 'statement',
      'target': Object.assign(q, {
                  'variant': s['format']
                })
    }, s);
  }

drop_start "DROP Keyword"
  = s:( DROP ) o t:( drop_types ) i:( drop_ie )?
  {
     return Object.assign({
       'variant': keyNode(s),
       'format': t,
       'condition': []
     }, i);
  }

drop_types "DROP Type"
  = t:( TABLE / INDEX / TRIGGER / VIEW ) o
  { return keyNode(t); }

drop_ie "IF EXISTS Keyword"
  = i:( IF ) o e:( EXISTS ) o
  {
    return {
      'condition': [{
        'type': 'condition',
        'variant': keyNode(i),
        'condition': {
          'type': 'expression',
          'variant': keyNode(e),
          'operator': keyNode(e)
        }
      }]
    };
  }

binary_concat "Or"
  = sym_pipe sym_pipe

binary_plus "Add"
  = sym_plus

binary_minus "Subtract"
  = sym_minus

binary_multiply "Multiply"
  = sym_star

binary_divide "Divide"
  = sym_fslash

binary_mod "Modulo"
  = sym_mod

binary_left "Shift Left"
  = sym_lt sym_lt

binary_right "Shift Right"
  = sym_gt sym_gt

binary_and "Logical AND"
  = sym_amp

binary_or "Logical OR"
  = sym_pipe

binary_lt "Less Than"
  = sym_lt

binary_gt "Greater Than"
  = sym_gt

binary_lte "Less Than Or Equal"
  = sym_lt sym_equal

binary_gte "Greater Than Or Equal"
  = sym_gt sym_equal

binary_equal "Equal"
  = sym_equal ( sym_equal )?

binary_notequal_a "Not Equal"
  = sym_excl sym_equal

binary_notequal_b "Not Equal"
  =  sym_lt sym_gt

binary_lang
  = binary_lang_isnt

binary_lang_isnt "IS"
  = i:( IS ) o n:( expression_is_not )?
  { return foldStringKey([ i, n ]); }

/* Database, Table and Column IDs */

id_name "Identifier"
  = name
  / name_reserved

id_database "Database Identifier"
  = n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'database',
      'name': n
    };
  }

id_function "Function Identifier"
  = d:( id_table_qualified )? n:( id_name ) {
    return {
      'type': 'identifier',
      // TODO: Should this be `table function` since it is table-function name
      'variant': 'function',
      'name': foldStringWord([ d, n ])
    };
  }

id_table "Table Identifier"
  = d:( id_table_qualified )? n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'table',
      'name': foldStringWord([ d, n ])
    };
  }

id_table_qualified
  = n:( id_name ) d:( sym_dot )
  { return foldStringWord([ n, d ]); }

id_column "Column Identifier"
  = q:( column_qualifiers / id_column_qualified / column_unqualified ) n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'column',
      'name': foldStringWord([ q, n ])
    };
  }

column_unqualified
  = o { return ''; }

column_qualifiers
  = d:( id_table_qualified ) t:( id_column_qualified )
  { return foldStringWord([ d, t ]); }

id_column_qualified
  = t:( id_name ) d:( sym_dot )
  { return foldStringWord([ t, d ]); }

/**
 * @note
 *   Datatype names are accepted as collation identifier in the
 *   reference implementation of SQLite.
 */
id_collation "Collation Identifier"
  = n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'collation',
      'name': n
    };
  }

id_savepoint "Savepoint Identifier"
  = n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'savepoint',
      'name': n
    };
  }

id_index "Index Identifier"
  = d:( id_table_qualified )? n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'index',
      'name': foldStringWord([ d, n ])
    };
  }

id_trigger "Trigger Identifier"
  = d:( id_table_qualified )? n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'trigger',
      'name': foldStringWord([ d, n ])
    };
  }

id_view "View Identifier"
  = d:( id_table_qualified )? n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'view',
      'name': foldStringWord([ d, n ])
    };
  }

id_pragma "Pragma Identifier"
  = d:( id_table_qualified )? n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'pragma',
      'name': foldStringWord([ d, n ])
    };
  }

id_cte "CTE Identifier"
  = d:( id_table_expression / id_table ) o {
    return d;
  }

id_table_expression
  = n:( id_table ) o a:( loop_columns ) {
    return Object.assign({
      'type': 'identifier',
      'variant': 'expression',
      'format': 'table',
      'name': n['name'],
      'columns': []
    }, a);
  }

id_constraint_table "Table Constraint Identifier"
  = n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'constraint',
      'format': 'table',
      'name': n
    };
  }

id_constraint_column "Column Constraint Identifier"
  = n:( id_name )
  {
    return {
      'type': 'identifier',
      'variant': 'constraint',
      'format': 'column',
      'name': n
    };
  }

/* Column datatypes */

datatype_types "Datatype Name"
  = t:( datatype_text ) !name_char { return [t, 'text']; }
  / t:( datatype_real ) !name_char { return [t, 'real']; }
  / t:( datatype_numeric ) !name_char { return [t, 'numeric']; }
  / t:( datatype_integer ) !name_char { return [t, 'integer']; }
  / t:( datatype_none ) !name_char { return [t, 'none']; }

datatype_text "TEXT Datatype Name"
  = t:( ( ( "N"i )? ( "VAR"i )? "CHAR"i )
  / ( ( "TINY"i / "MEDIUM"i / "LONG"i )? "TEXT"i )
  / "CLOB"i )
  { return keyNode(t); }

datatype_real "REAL Datatype Name"
  = t:( datatype_real_double / "FLOAT"i / "REAL"i )
  { return keyNode(t); }

datatype_real_double "DOUBLE Datatype Name"
  = d:( "DOUBLE"i ) p:( [\t ]+ "PRECISION"i )?
  { return foldStringWord([ d, p ]); }

datatype_numeric "NUMERIC Datatype Name"
  = t:( "NUMERIC"i
  / "DECIMAL"i
  / "BOOLEAN"i
  / ( "DATE"i ( "TIME"i )? )
  / ( "TIME"i ( "STAMP"i )? )
  / "STRING"i )
  { return keyNode(t); }

datatype_integer "INTEGER Datatype Name"
  = t:( ( "INT"i ( "2" / "4" / "8" / "EGER"i ) )
  / ( ( "BIG"i / "MEDIUM"i / "SMALL"i / "TINY"i )? "INT"i )
  / datatype_integer_fp )
  { return keyNode(t); }
datatype_integer_fp
  = f:( "FLOATING"i ) p:( [\t ]+ "POINT"i ) {
    return foldStringWord([ f, p ]);
  }

datatype_none "BLOB Datatype Name"
  = t:( "BLOB"i )
  { return keyNode(t); }

/* Naming rules */

/**
 * @note
 *  This is a best approximation of the characters allowed in an unquoted
 *  identifier or alias.
 */
name_char
  = [a-z0-9\$\_]i

unicode_char
  = u:( "\\u" ) s:( [a-f0-9]i+ ) {
  return foldStringWord([ u, s ]).toLowerCase();
}

/**
* @note
*  Since SQLite is tolerant of this behavior, although it is non-standard,
*  parser allows single-quoted string literals to be interpreted as aliases.
*/
name
  = name_quoted
  / name_unquoted

name_quoted
  = name_bracketed
  / name_backticked
  / name_dblquoted
  / name_sglquoted

name_unquoted
  = !( reserved_words / number_digit ) n:( unicode_char / name_char )+ {
    return keyNode(n);
  }

/**
 * @note
 *   This is for places where reserved words can be used as unquoted
 *   identifiers to mimic the native SQLite parser behavior.
 */
name_reserved
  = !( reserved_critical_list / number_digit ) n:( unicode_char / name_char )+ {
   return keyNode(n);
  }

/** @note Non-standard legacy format */
name_bracketed
  = sym_bopen o n:$( !bracket_terminator . )* bracket_terminator {
    return textNode(n);
  }
bracket_terminator
  = [ \t]* sym_bclose o

name_dblquoted
  = '"' n:( '""' / [^\"] )* '"'
  { return unescape(n, '"'); }

/** @note Non-standard format */
name_sglquoted
  = "'" n:( "''" / [^\'] )* "'"
  { return unescape(n, "'"); }

/** @note Non-standard legacy format */
name_backticked
  = '`' n:( '``' / [^\`] )* '`'
  { return unescape(n, '`'); }

/* Symbols */

sym_bopen "Open Bracket"
  = s:( "[" ) o { return s; }
sym_bclose "Close Bracket"
  = s:( "]" ) o { return s; }
sym_popen "Open Parenthesis"
  = s:( "(" ) o { return s; }
sym_pclose "Close Parenthesis"
  = s:( ")" ) o { return s; }
sym_comma "Comma"
  = s:( "," ) o { return s; }
sym_dot "Period"
  = s:( "." ) o { return s; }
sym_star "Asterisk"
  = s:( "*" ) o { return s; }
sym_quest "Question Mark"
  = s:( "?" ) o { return s; }
sym_sglquote "Single Quote"
  = s:( "'" ) o { return s; }
sym_dblquote "Double Quote"
  = s:( '"' ) o { return s; }
sym_backtick "Backtick"
  = s:( "`" ) o { return s; }
sym_tilde "Tilde"
  = s:( "~" ) o { return s; }
sym_plus "Plus"
  = s:( "+" ) o { return s; }
sym_minus "Minus"
  = s:( "-" ) o { return s; }
sym_equal "Equal"
  = s:( "=" ) o { return s; }
sym_amp "Ampersand"
  = s:( "&" ) o { return s; }
sym_pipe "Pipe"
  = s:( "|" ) o { return s; }
sym_mod "Modulo"
  = s:( "%" ) o { return s; }
sym_lt "Less Than"
  = s:( "<" ) o { return s; }
sym_gt "Greater Than"
  = s:( ">" ) o { return s; }
sym_excl "Exclamation"
  = s:( "!" ) o { return s; }
sym_semi "Semicolon"
  = s:( ";" ) o { return s; }
sym_colon "Colon"
  = s:( ":" ) o { return s; }
sym_fslash "Forward Slash"
  = s:( "/" ) o { return s; }
sym_bslash "Backslash"
  = s:( "\\" ) o { return s; }

/* Keywords */

ABORT
  = "ABORT"i !name_char
ACTION
  = "ACTION"i !name_char
ADD
  = "ADD"i !name_char
AFTER
  = "AFTER"i !name_char
ALL
  = "ALL"i !name_char
ALTER
  = "ALTER"i !name_char
ANALYZE
  = "ANALYZE"i !name_char
AND
  = "AND"i !name_char
AS
  = "AS"i !name_char
ASC
  = "ASC"i !name_char
ATTACH
  = "ATTACH"i !name_char
AUTOINCREMENT
  = "AUTOINCREMENT"i !name_char
BEFORE
  = "BEFORE"i !name_char
BEGIN
  = "BEGIN"i !name_char
BETWEEN
  = "BETWEEN"i !name_char
BY
  = "BY"i !name_char
CASCADE
  = "CASCADE"i !name_char
CASE
  = "CASE"i !name_char
CAST
  = "CAST"i !name_char
CHECK
  = "CHECK"i !name_char
COLLATE
  = "COLLATE"i !name_char
COLUMN
  = "COLUMN"i !name_char
COMMIT
  = "COMMIT"i !name_char
CONFLICT
  = "CONFLICT"i !name_char
CONSTRAINT
  = "CONSTRAINT"i !name_char
CREATE
  = "CREATE"i !name_char
CROSS
  = "CROSS"i !name_char
CURRENT_DATE
  = "CURRENT_DATE"i !name_char
CURRENT_TIME
  = "CURRENT_TIME"i !name_char
CURRENT_TIMESTAMP
  = "CURRENT_TIMESTAMP"i !name_char
DATABASE
  = "DATABASE"i !name_char
DEFAULT
  = "DEFAULT"i !name_char
DEFERRABLE
  = "DEFERRABLE"i !name_char
DEFERRED
  = "DEFERRED"i !name_char
DELETE
  = "DELETE"i !name_char
DESC
  = "DESC"i !name_char
DETACH
  = "DETACH"i !name_char
DISTINCT
  = "DISTINCT"i !name_char
DROP
  = "DROP"i !name_char
EACH
  = "EACH"i !name_char
ELSE
  = "ELSE"i !name_char
END
  = "END"i !name_char
ESCAPE
  = "ESCAPE"i !name_char
EXCEPT
  = "EXCEPT"i !name_char
EXCLUSIVE
  = "EXCLUSIVE"i !name_char
EXISTS
  = "EXISTS"i !name_char
EXPLAIN
  = "EXPLAIN"i !name_char
FAIL
  = "FAIL"i !name_char
FOR
  = "FOR"i !name_char
FOREIGN
  = "FOREIGN"i !name_char
FROM
  = "FROM"i !name_char
FULL
  = "FULL"i !name_char
GLOB
  = "GLOB"i !name_char
GROUP
  = "GROUP"i !name_char
HAVING
  = "HAVING"i !name_char
IF
  = "IF"i !name_char
IGNORE
  = "IGNORE"i !name_char
IMMEDIATE
  = "IMMEDIATE"i !name_char
IN
  = "IN"i !name_char
INDEX
  = "INDEX"i !name_char
INDEXED
  = "INDEXED"i !name_char
INITIALLY
  = "INITIALLY"i !name_char
INNER
  = "INNER"i !name_char
INSERT
  = "INSERT"i !name_char
INSTEAD
  = "INSTEAD"i !name_char
INTERSECT
  = "INTERSECT"i !name_char
INTO
  = "INTO"i !name_char
IS
  = "IS"i !name_char
ISNULL
  = "ISNULL"i !name_char
JOIN
  = "JOIN"i !name_char
KEY
  = "KEY"i !name_char
LEFT
  = "LEFT"i !name_char
LIKE
  = "LIKE"i !name_char
LIMIT
  = "LIMIT"i !name_char
MATCH
  = "MATCH"i !name_char
NATURAL
  = "NATURAL"i !name_char
NO
  = "NO"i !name_char
NOT
  = "NOT"i !name_char
NOTNULL
  = "NOTNULL"i !name_char
NULL
  = "NULL"i !name_char
OF
  = "OF"i !name_char
OFFSET
  = "OFFSET"i !name_char
ON
  = "ON"i !name_char
OR
  = "OR"i !name_char
ORDER
  = "ORDER"i !name_char
OUTER
  = "OUTER"i !name_char
PLAN
  = "PLAN"i !name_char
PRAGMA
  = "PRAGMA"i !name_char
PRIMARY
  = "PRIMARY"i !name_char
QUERY
  = "QUERY"i !name_char
RAISE
  = "RAISE"i !name_char
RECURSIVE
  = "RECURSIVE"i !name_char
REFERENCES
  = "REFERENCES"i !name_char
REGEXP
  = "REGEXP"i !name_char
REINDEX
  = "REINDEX"i !name_char
RELEASE
  = "RELEASE"i !name_char
RENAME
  = "RENAME"i !name_char
REPLACE
  = "REPLACE"i !name_char
RESTRICT
  = "RESTRICT"i !name_char
RIGHT
  = "RIGHT"i !name_char
ROLLBACK
  = "ROLLBACK"i !name_char
ROW
  = "ROW"i !name_char
ROWID
  = "ROWID"i !name_char
SAVEPOINT
  = "SAVEPOINT"i !name_char
SELECT
  = "SELECT"i !name_char
SET
  = "SET"i !name_char
TABLE
  = "TABLE"i !name_char
TEMP
  = "TEMP"i !name_char
TEMPORARY
  = "TEMPORARY"i !name_char
THEN
  = "THEN"i !name_char
TO
  = "TO"i !name_char
TRANSACTION
  = "TRANSACTION"i !name_char
TRIGGER
  = "TRIGGER"i !name_char
UNION
  = "UNION"i !name_char
UNIQUE
  = "UNIQUE"i !name_char
UPDATE
  = "UPDATE"i !name_char
USING
  = "USING"i !name_char
VACUUM
  = "VACUUM"i !name_char
VALUES
  = "VALUES"i !name_char
VIEW
  = "VIEW"i !name_char
VIRTUAL
  = "VIRTUAL"i !name_char
WHEN
  = "WHEN"i !name_char
WHERE
  = "WHERE"i !name_char
WITH
  = "WITH"i !name_char
WITHOUT
  = "WITHOUT"i !name_char

reserved_words
  = r:( reserved_word_list )
  { return keyNode(r); }

/**
 * @note
 *   CROSS, RELEASE, ROWID, and TEMP removed here to be used as table
 *   and column names.
 */
reserved_word_list
  = ABORT / ACTION / ADD / AFTER / ALL / ALTER / ANALYZE / AND / AS /
    ASC / ATTACH / AUTOINCREMENT / BEFORE / BEGIN / BETWEEN / BY /
    CASCADE / CASE / CAST / CHECK / COLLATE / COLUMN / COMMIT /
    CONFLICT / CONSTRAINT / CREATE / CROSS / CURRENT_DATE /
    CURRENT_TIME / CURRENT_TIMESTAMP / DATABASE / DEFAULT /
    DEFERRABLE / DEFERRED / DELETE / DESC / DETACH / DISTINCT /
    DROP / EACH / ELSE / END / ESCAPE / EXCEPT / EXCLUSIVE / EXISTS /
    EXPLAIN / FAIL / FOR / FOREIGN / FROM / FULL / GLOB / GROUP /
    HAVING / IF / IGNORE / IMMEDIATE / IN / INDEX / INDEXED /
    INITIALLY / INNER / INSERT / INSTEAD / INTERSECT / INTO / IS /
    ISNULL / JOIN / KEY / LEFT / LIKE / LIMIT / MATCH / NATURAL /
    NO / NOT / NOTNULL / NULL / OF / OFFSET / ON / OR / ORDER /
    OUTER / PLAN / PRAGMA / PRIMARY / QUERY / RAISE / RECURSIVE /
    REFERENCES / REGEXP / REINDEX / RELEASE / RENAME / REPLACE /
    RESTRICT / RIGHT / ROLLBACK / ROW / SAVEPOINT / SELECT /
    SET / TABLE / TEMPORARY / THEN / TO / TRANSACTION /
    TRIGGER / UNION / UNIQUE / UPDATE / USING / VACUUM / VALUES /
    VIEW / VIRTUAL / WHEN / WHERE / WITH / WITHOUT

/**
 * @note
 *   Not all reserved words are created equal in SQLite as these are
 *   words that cannot be used as an unquoted column identifer while
 *   the words on the master list (reserved_word_list) that do not
 *   also appear here _can_ be used as column or table names.
 */
reserved_critical_list
  = ADD / ALL / ALTER / AND / AS / AUTOINCREMENT / BETWEEN / CASE /
    CHECK / COLLATE / COMMIT / CONSTRAINT / CREATE / DEFAULT /
    DEFERRABLE / DELETE / DISTINCT / DROP / ELSE / ESCAPE / EXCEPT /
    EXISTS / FOREIGN / FROM / GROUP / HAVING / IN / INDEX / INSERT /
    INTERSECT / INTO / IS / ISNULL / JOIN / LIMIT / NOT / NOTNULL /
    NULL / ON / OR / ORDER / PRIMARY / REFERENCES / SELECT / SET /
    TABLE / THEN / TO / TRANSACTION / UNION / UNIQUE / UPDATE /
    USING / VALUES / WHEN / WHERE

/* Generic rules */

/* TODO: Not returning anything in AST for comments, should decide what to do with them */
comment
  = comment_line
  / comment_block
  { return null; }

comment_line "Line Comment"
  = "--" ( ![\n\v\f\r] . )*

comment_block "Block Comment"
  = comment_block_start comment_block_feed comment_block_end

comment_block_start
  = "/*"

comment_block_end
  = "*/"

comment_block_body
  = ( !( comment_block_end / comment_block_start ) . )+

block_body_nodes
  = comment_block_body / comment_block

comment_block_feed
  = block_body_nodes ( [\n\v\f\r\t ] / block_body_nodes )*

/* Optional Whitespace */
o "Whitespace"
  = n:( [\n\v\f\r\t ] / comment )*
  { return n; }

/* TODO: Everything with this symbol */
_TODO_
  = "__TODO__"
