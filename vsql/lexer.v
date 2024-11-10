// lexer.v contains the lexer (tokenizer) that convert a SQL string into tokens
// to be read by the parser.v

module vsql

// Except for the eof and the keywords, the other tokens use the names described
// in the SQL standard.
enum TokenKind {
	asterisk                        // <asterisk> ::= *
	colon                           // <colon> ::= :
	comma                           // <comma> ::= ,
	concatenation_operator          // <concatenation operator> ::= ||
	equals_operator                 // <equals operator> ::= =
	greater_than_operator           // <greater than operator> ::= >
	greater_than_or_equals_operator // <greater than or equals operator> ::= >=
	keyword
	left_paren                   // <left paren> ::= (
	less_than_operator           // <less than operator> ::= <
	less_than_or_equals_operator // <less than or equals operator> ::= <=
	literal_identifier           // foo or "foo" (delimited)
	literal_number               // 123
	literal_string               // 'hello'
	minus_sign                   // <minus sign> ::= -
	not_equals_operator          // <not equals operator> ::= <>
	period                       // <period> ::= .
	plus_sign                    // <plus sign> ::= +
	right_paren                  // <right paren> ::= )
	semicolon                    // <semicolon> ::= ;
	solidus                      // <solidus> ::= /
}

struct Token {
	token int
	sym   YYSymType
}

fn tokenize(sql_stmt string) []Token {
	mut tokens := []Token{}
	cs := sql_stmt.trim(';').runes()
	mut i := 0

	for i < cs.len {
		// Numbers
		if cs[i] >= `0` && cs[i] <= `9` {
			mut word := ''
			for i < cs.len && cs[i] >= `0` && cs[i] <= `9` {
				word += '${cs[i]}'
				i++
			}
			tokens << Token{token_literal_number, YYSymType{
				v: word
			}}

			// There is a special case for approximate numbers where 'E' is considered
			// a separate token in the SQL BNF. However, "e2" should not be treated as
			// two tokens, but rather we need to catch this case only when with a
			// number token.
			if i < cs.len && (cs[i] == `e` || cs[i] == `E`) {
				tokens << Token{token_e, YYSymType{}}
				i++
			}

			unsafe {
				goto next
			}
		}

		// Strings
		if cs[i] == `'` {
			mut word := ''
			i++
			for i < cs.len && cs[i] != `'` {
				word += '${cs[i]}'
				i++
			}
			i++
			tokens << Token{token_literal_string, YYSymType{
				v: new_character_value(word)
			}}
			unsafe {
				goto next
			}
		}

		// Delimited identifiers
		if cs[i] == `"` {
			mut word := ''
			i++
			for i < cs.len && cs[i] != `"` {
				word += '${cs[i]}'
				i++
			}
			i++
			tokens << Token{token_literal_identifier, YYSymType{
				v: IdentifierChain{
					identifier: '"${word}"'
				}
			}}
			unsafe {
				goto next
			}
		}

		// Operators
		multi := {
			'<>': token_operator_not_equals
			'>=': token_operator_greater_equals
			'<=': token_operator_less_equals
			'||': token_operator_double_pipe
		}
		for op, tk in multi {
			if i < cs.len - 1 && cs[i] == op[0] && cs[i + 1] == op[1] {
				tokens << Token{tk, YYSymType{
					v: op
				}}
				i += 2
				unsafe {
					goto next
				}
			}
		}

		single := {
			`(`: token_operator_left_paren
			`)`: token_operator_right_paren
			`*`: token_operator_asterisk
			`+`: token_operator_plus
			`,`: token_operator_comma
			`-`: token_operator_minus
			`/`: token_operator_solidus
			`;`: token_operator_semicolon
			`<`: token_operator_less_than
			`=`: token_operator_equals
			`>`: token_operator_greater_than
			`.`: token_operator_period
			`:`: token_operator_colon
		}
		for op, tk in single {
			if cs[i] == op {
				tokens << Token{tk, YYSymType{
					v: op.str()
				}}
				i++
				unsafe {
					goto next
				}
			}
		}

		// Keyword or regular identifier
		mut word := ''
		mut is_not_first := false
		for i < cs.len && is_identifier_char(cs[i], is_not_first) {
			word += '${cs[i]}'
			i++
			is_not_first = true
		}

		if word == '' {
			i++
			continue
		}

		upper_word := word.to_upper()
		mut found := false
		for tok_pos, tok_name in yy_toknames {
			if tok_name == upper_word {
				tok_number := tok_pos + 57343
				tokens << Token{tok_number, YYSymType{
					v: upper_word
				}}
				found = true
				break
			}
		}

		if !found {
			tokens << Token{token_literal_identifier, YYSymType{
				v: IdentifierChain{
					identifier: word
				}
			}}
		}

		next:
		length, new_token := tail_substitution(tokens)
		if length > 0 {
			tokens = tokens[..tokens.len - length].clone()
			tokens << Token{new_token, YYSymType{}}
		}
	}

	return tokens
}

@[inline]
fn is_identifier_char(c rune, is_not_first bool) bool {
	yes := (c >= `a` && c <= `z`) || (c >= `A` && c <= `Z`) || c == `_`

	if is_not_first {
		return yes || (c >= `0` && c <= `9`)
	}

	return yes
}

fn tail_substitution(tokens []Token) (int, int) {
	len := tokens.len

	if len > 2 && tokens[len - 2].token == token_operator_period
		&& tokens[len - 1].token == token_operator_asterisk {
		return 2, token_operator_period_asterisk
	}
	if len > 2 && tokens[len - 2].token == token_operator_left_paren
		&& tokens[len - 1].token == token_operator_asterisk {
		return 2, token_operator_left_paren_asterisk
	}
	if len > 2 && tokens[len - 2].token == token_is && tokens[len - 1].token == token_true {
		return 2, token_is_true
	}
	if len > 2 && tokens[len - 2].token == token_is && tokens[len - 1].token == token_false {
		return 2, token_is_false
	}
	if len > 2 && tokens[len - 2].token == token_is && tokens[len - 1].token == token_unknown {
		return 2, token_is_unknown
	}
	if len > 3 && tokens[len - 3].token == token_is && tokens[len - 2].token == token_not
		&& tokens[len - 1].token == token_true {
		return 3, token_is_not_true
	}
	if len > 3 && tokens[len - 3].token == token_is && tokens[len - 2].token == token_not
		&& tokens[len - 1].token == token_false {
		return 3, token_is_not_false
	}
	if len > 3 && tokens[len - 3].token == token_is && tokens[len - 2].token == token_not
		&& tokens[len - 1].token == token_unknown {
		return 3, token_is_not_unknown
	}

	return 0, 0
}
