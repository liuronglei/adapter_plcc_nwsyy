// use eig_expr::scipt::ast::Prog;
// use eig_expr::ParseError;
// use std::path::Path;
// use std::{fmt, fs};


use crate::model::south::*;
// use crate::scipt::ast::*;
use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::str::Chars;
use unic_ucd::GeneralCategory;

pub struct Parser<'a> {
    pub chs: Chars<'a>,
    pub peeked: VecDeque<char>,
    pub line: i32,
    pub column: i32,
    in_loop_flags: Vec<bool>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser {
            chs: input.chars(),
            peeked: Default::default(),
            line: 1,
            column: 0,
            in_loop_flags: vec![],
        }
    }

    fn enter_loop(&mut self) {
        self.in_loop_flags.push(true);
    }

    fn leave_loop(&mut self) {
        self.in_loop_flags.pop();
    }

    fn in_loop(&self) -> bool {
        match self.in_loop_flags.last() {
            Some(_) => true,
            _ => false,
        }
    }

    pub fn prog(&mut self) -> Result<Prog, ParseError> {
        let mut body = vec![];
        loop {
            self.skip_whitespace_or_comment();
            if !self.ahead_is_eof() {
                let stmt = self.stmt()?;
                body.push(stmt);
            } else {
                break;
            }
        }
        Ok(Prog { body })
    }

    fn stmt(&mut self) -> Result<Stmt, ParseError> {
        self.skip_whitespace_or_comment();
        if self.ahead_is_symbol('{') {
            self.block_stmt()
        } else if self.ahead_is_keyword("if") {
            self.if_stmt()
        } else if self.ahead_is_keyword("for") {
            self.for_stmt()
        } else if self.ahead_is_keyword("while") {
            self.while_stmt()
        } else if self.ahead_is_keyword("loop") {
            self.loop_stmt()
        } else if self.ahead_is_keyword("break") {
            self.break_stmt()
        } else if self.ahead_is_keyword("continue") {
            self.continue_stmt()
        } else if self.ahead_is_keyword("return") {
            self.return_stmt()
        } else if self.ahead_is_keyword("fn") {
            self.fn_dec_stmt()
        } else if self.ahead_is_id_start() {
            // var declare or expression stmt
            let mut loc = self.loc();
            let name = self.read_name()?;
            self.skip_whitespace_or_comment();
            let next_c = self.peek();
            if (next_c == Some('=') || next_c == Some(':')) && !self.ahead_is_keyword("==") {
                self.advance();
                let expr = self.read_expr(String::new(), false)?;
                loc.end = self.pos();
                Ok(Stmt::VarDec(VarDecStmt { loc, id: name, init: expr, }.into()))
            } else {
                // call function
                let expr = self.read_expr(name, false)?;
                loc.end = self.pos();
                Ok(Stmt::CallExpr(CallExprStmt { loc, expr }.into()))
            }
        } else {
            self.call_expr_stmt()
        }
    }

    fn ahead_is_symbol(&mut self, syb: char) -> bool {
        if let Some(c) = self.peek() {
            c == syb
        } else {
            false
        }
    }

    fn ahead_is_keyword(&mut self, k: &str) -> bool {
        if self.ahead_is_id_start() {
            match self.read_name() {
                Ok(s) => {
                    let r = s == k;
                    let mut chars = s.chars();
                    loop {
                        if let Some(c) = chars.next_back() {
                            self.peeked.push_front(c);
                        } else {
                            break;
                        }
                    }
                    r
                }
                Err(_) => {
                    false
                }
            }
        } else {
            false
        }
    }

    fn if_stmt(&mut self) -> Result<Stmt, ParseError> {
        let mut loc = self.loc();
        self.advance2();
        let test = self.read_expr(String::new(), false)?;
        self.skip_whitespace_or_comment();
        if Some('{') != self.peek() {
            return Err(ParseError::UnexpectedToken(
                self.line as usize,
                self.column as usize,
            ))
        }
        let body = self.block_stmt()?;
        self.skip_whitespace_or_comment();
        let alt = if self.ahead_is_keyword("else") {
            self.advance_n(4);
            Some(self.stmt()?)
        } else {
            None
        };
        loc.end = self.pos();
        Ok(Stmt::If(IfStmt { loc, test, body, alt }.into()))
    }

    fn for_stmt(&mut self) -> Result<Stmt, ParseError> {
        self.enter_loop();
        let mut loc = self.loc();
        self.advance_n(3);
        self.skip_whitespace_or_comment();
        let mut init: Option<VarDecStmt> = None;
        let mut test: Option<Expr> = None;
        let mut update: Option<Expr> = None;
        let body:Stmt;
        if let Some(';') = self.peek() {
            self.advance();
            self.skip_whitespace_or_comment();
            if Some(';') == self.peek() {
                self.advance();
                self.skip_whitespace_or_comment();
                if Some('{') == self.peek() {
                    body = self.block_stmt()?;
                } else {
                    update = Some(self.read_expr(String::new(), false)?);
                    self.skip_whitespace_or_comment();
                    if Some('{') != self.peek() {
                        return Err(ParseError::UnexpectedToken(
                            self.line as usize,
                            self.column as usize,
                        ))
                    }
                    body = self.block_stmt()?;
                    loc.end = self.pos();
                }
            } else {
                test = Some(self.read_expr(String::new(), false)?);
                self.skip_whitespace_or_comment();
                if Some('{') == self.peek() {
                    body = self.block_stmt()?;
                } else {
                    update = Some(self.read_expr(String::new(), false)?);
                    self.skip_whitespace_or_comment();
                    if Some('{') != self.peek() {
                        return Err(ParseError::UnexpectedToken(
                            self.line as usize,
                            self.column as usize,
                        ))
                    }
                    body = self.block_stmt()?;
                }
            }
            loc.end = self.pos();
            self.leave_loop();
            Ok(Stmt::For(ForStmt { loc, init, test, update, body}.into()))
        } else {
            let name = self.read_name()?;
            self.skip_whitespace_or_comment();
            if self.ahead_is_keyword("in") {
                self.advance2();
                let start = self.read_expr(String::new(), true)?;
                self.skip_whitespace_or_comment();
                let next_three = self.peek_n(3);
                if next_three[0] != Some('.') || next_three[1] != Some('.') {
                    return Err(ParseError::UnexpectedToken(
                        self.line as usize,
                        self.column as usize,
                    ))
                }
                let exhausted = if next_three[2] == Some('=') {
                    self.advance_n(3);
                    true
                } else {
                    self.advance2();
                  false
                };
                let end = self.read_expr(String::new(), false)?;
                self.skip_whitespace_or_comment();
                if Some('{') != self.peek() {
                    return Err(ParseError::UnexpectedToken(
                        self.line as usize,
                        self.column as usize,
                    ))
                }
                body = self.block_stmt()?;
                loc.end = self.pos();
                self.leave_loop();
                Ok(Stmt::ForIn(ForInStmt { loc, left: name, start, end, exhausted, body}.into()))
            } else {
                let next_c = self.peek();
                if next_c == Some('=') || next_c == Some(':') {
                    self.advance();
                    let expr = self.read_expr(String::new(), false)?;
                    loc.end = self.pos();
                    init = Some(VarDecStmt { loc, id: name, init: expr})
                } else {
                    return Err(ParseError::UnexpectedToken(
                        self.line as usize,
                        self.column as usize,
                    ));
                }
                self.skip_whitespace_or_comment();
                if Some(';') != self.peek() {
                    test = Some(self.read_expr(String::new(), false)?);
                }
                self.skip_whitespace_or_comment();
                if Some('{') != self.peek() {
                    update = Some(self.read_expr(String::new(), false)?);
                }
                self.skip_whitespace_or_comment();
                if Some('{') != self.peek() {
                    return Err(ParseError::UnexpectedToken(
                        self.line as usize,
                        self.column as usize,
                    ))
                }
                body = self.block_stmt()?;
                loc.end = self.pos();
                self.leave_loop();
                Ok(Stmt::For(ForStmt { loc, init, test, update, body}.into()))
            }
        }
    }

    fn while_stmt(&mut self) -> Result<Stmt, ParseError> {
        self.enter_loop();
        let mut loc = self.loc();
        self.advance_n(5);
        let test = self.read_expr(String::new(), false)?;
        self.skip_whitespace_or_comment();
        if Some('{') != self.peek() {
            return Err(ParseError::UnexpectedToken(
                self.line as usize,
                self.column as usize,
            ))
        }
        let body = self.block_stmt()?;
        loc.end = self.pos();
        self.leave_loop();
        Ok(Stmt::While(WhileStmt { loc, test, body}.into()))
    }

    fn loop_stmt(&mut self) -> Result<Stmt, ParseError> {
        self.enter_loop();
        let mut loc = self.loc();
        self.advance_n(4);
        self.skip_whitespace_or_comment();
        if Some('{') != self.peek() {
            return Err(ParseError::UnexpectedToken(
                self.line as usize,
                self.column as usize,
            ))
        }
        let body = self.block_stmt()?;
        loc.end = self.pos();
        self.leave_loop();
        Ok(Stmt::Loop(LoopStmt { loc, body}.into()))
    }

    fn break_stmt(&mut self) -> Result<Stmt, ParseError> {
        if !self.in_loop() {
            return Err(ParseError::UnexpectedToken(
                self.line as usize,
                self.column as usize,
            ));
        }

        let mut loc = self.loc();
        self.advance_n(5);
        self.skip_whitespace_or_comment();
        loc.end = self.pos();
        if self.ahead_is_symbol(';') {
            self.advance();
        }
        Ok(Stmt::Break(BreakStmt { loc }.into()))
    }

    fn continue_stmt(&mut self) -> Result<Stmt, ParseError> {
        if !self.in_loop() {
            return Err(ParseError::UnexpectedToken(
                self.line as usize,
                self.column as usize,
            ));
        }
        let mut loc = self.loc();
        self.advance_n(8);
        self.skip_whitespace_or_comment();
        loc.end = self.pos();
        if self.ahead_is_symbol(';') {
            self.advance();
        }
        loc.end = self.pos();
        Ok(Stmt::Continue(ContinueStmt { loc }.into()))
    }

    fn return_stmt(&mut self) -> Result<Stmt, ParseError> {
        let loc = self.loc();
        self.advance_n(6);
        let mut ret = ReturnStmt {
            loc,
            argument: None,
        };
        self.skip_whitespace_or_comment();
        if !self.ahead_is_symbol('{') {
            if self.ahead_is_symbol(';') {
                self.advance();
            } else {
                ret.argument = Some(self.read_expr(String::new(), false)?);
            }
        }

        ret.loc.end = self.pos();
        Ok(Stmt::Return(ret.into()))
    }

    fn fn_dec_stmt(&mut self) -> Result<Stmt, ParseError> {
        let mut loc = self.loc();
        // skip "fn"
        self.advance2();
        self.skip_whitespace_or_comment();
        let id = self.read_name()?;
        self.skip_whitespace_or_comment();
        if Some('(') != self.peek() {
            return Err(ParseError::UnexpectedToken(
                self.line as usize,
                self.column as usize,
            ))
        }
        self.advance();
        let mut params = vec![];
        loop {
            self.skip_whitespace_or_comment();
            if self.ahead_is_symbol(',') {
                self.advance();
            } else {
                if self.ahead_is_symbol(')') {
                    self.advance();
                    break;
                } else {
                    let para = self.read_name()?;
                    params.push(para);
                }
            }
        }
        self.skip_whitespace_or_comment();
        if Some('{') != self.peek() {
            return Err(ParseError::UnexpectedToken(
                self.line as usize,
                self.column as usize,
            ))
        }
        let body = self.block_stmt()?;
        loc.end = self.pos();
        Ok(Stmt::Function(FnDecStmt { loc, id, params, body}.into()))
    }

    fn block_stmt(&mut self) -> Result<Stmt, ParseError> {
        let mut loc = self.loc();
        let mut body = vec![];
        self.advance();
        loop {
            self.skip_whitespace_or_comment();
            let c = self.peek().ok_or(ParseError::UnexpectedToken(
                self.line as usize,
                self.column as usize,
            ))?;
            if c == '}' {
                self.advance();
                break;
            }
            body.push(self.stmt()?);
        }
        loc.end = self.pos();
        Ok(Stmt::Block(BlockStmt { loc, body }.into()))
    }

    fn call_expr_stmt(&mut self) -> Result<Stmt, ParseError> {
        let mut loc = self.loc();
        let expr = self.read_expr(String::new(), false)?;
        loc.end = self.pos();
        Ok(Stmt::CallExpr(CallExprStmt { loc, expr }.into()))
    }

    fn read_name(&mut self) -> Result<String, ParseError> {
        let char = self.read().unwrap();
        let c = self.read_escape_unicode(char)?;
        let mut val = vec![c];
        let cc = self.read_id_part()?;
        val.extend(cc.chars());
        Ok(val.into_iter().collect())
    }

    fn read_expr(&mut self, mut start: String, in_start: bool) -> Result<Expr, ParseError> {
        let loc = self.loc();
        let a = vec![Some('.'), Some('.')];
        loop {
            // expression in for a..b
            let v = self.peek_n(2);
            if in_start && v == a {
                break;
            }
            match self.peek() {
                Some(c) => {
                    if c == ';' {
                        self.advance();
                        break;
                    } else if c == '}' || c == '{' {
                        break;
                    } else {
                        start.push(self.read().unwrap());
                    }
                }
                _ => break,
            }
        }
        let expr = start
            .parse::<Expr>()
            .map_err(|_| ParseError::UnexpectedToken(loc.start.line as usize, loc.start.column as usize))?;
        if !expr.check_validity() {
            Err(ParseError::UnexpectedToken(loc.start.line as usize, loc.start.column as usize))
        } else {
            Ok(expr)
        }
    }

    fn read_id_part(&mut self) -> Result<String, ParseError> {
        let mut val = vec![];
        loop {
            if self.ahead_is_id_part() {
                let c = self.read().unwrap();
                let cc = self.read_escape_unicode(c)?;
                val.push(cc);
            } else {
                break;
            }
        }
        Ok(val.into_iter().collect())
    }

    fn read_escape_unicode(&mut self, bs: char) -> Result<char, ParseError> {
        if bs == '\\' && self.test_ahead('u') {
            self.advance();
            match self.read_unicode_escape_seq() {
                Some(ec) => Ok(ec),
                _ => Err(ParseError::UnexpectedToken(
                    self.line as usize,
                    self.column as usize,
                )),
            }
        } else {
            Ok(bs)
        }
    }

    fn read_unicode_escape_seq(&mut self) -> Option<char> {
        let mut hex = [0, 0, 0, 0];
        for i in 0..hex.len() {
            match self.read() {
                Some(c) => {
                    if c.is_ascii_hexdigit() {
                        hex[i] = c as u8;
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        let hex = std::str::from_utf8(&hex).unwrap();
        match u32::from_str_radix(hex, 16) {
            Ok(i) => match char::from_u32(i) {
                Some(c) => Some(c),
                _ => None, // deformed unicode
            }
            _ => None, // deformed hex digits
        }
    }

    fn skip_whitespace_or_comment(&mut self) {
        loop {
            if self.ahead_is_whitespace_or_line_terminator() {
                self.read();
            } else if self.test_ahead2('/', '/') {
                self.skip_comment_single();
            } else if self.test_ahead2('/', '*') {
                self.skip_comment_multi();
            } else {
                break;
            }
        }
    }

    fn ahead_is_whitespace_or_line_terminator(&mut self) -> bool {
        match self.peek() {
            Some(c) => c.is_whitespace(),
            _ => false,
        }
    }

    fn next_join_crlf(&mut self) -> Option<char> {
        match self.chs.next() {
            Some(c) => {
                if is_line_terminator(c) {
                    if c == '\r' {
                        if let Some(c) = self.chs.next() {
                            if c != '\n' {
                                self.peeked.push_back(c);
                            }
                        }
                    }
                    Some('\n')
                } else {
                    Some(c)
                }
            }
            _ => None,
        }
    }
    fn read(&mut self) -> Option<char> {
        let c = match self.peeked.pop_front() {
            Some(c) => Some(c),
            _ => self.next_join_crlf(),
        };
        if let Some(c) = c {
            if c == '\n' {
                self.line += 1;
                self.column = 0;
            } else {
                self.column += 1;
            }
        }
        c
    }

    fn peek(&mut self) -> Option<char> {
        match self.peeked.front().cloned() {
            Some(c) => Some(c),
            _ => match self.next_join_crlf() {
                Some(c) => {
                    self.peeked.push_back(c);
                    Some(c)
                }
                _ => None,
            }
        }
    }

    fn peek_n(&mut self, n: usize) -> Vec<Option<char>> {
        let mut r = Vec::with_capacity(n);
        for _ in 0..n {
            match self.peeked.pop_front() {
                Some(c) => r.push(Some(c)),
                _ => match self.next_join_crlf() {
                    Some(c) => {
                        r.push(Some(c))
                    }
                    _ => {
                        r.push(None);
                        break;
                    }
                }
            }
        }
        for i in 0..r.len() {
            if let Some(Some(c)) = r.get(r.len() - i - 1) {
                self.peeked.push_front(*c);
            }
        }
        r
    }

    fn test_ahead(&mut self, ch: char) -> bool {
        match self.peek() {
            Some(c) => c == ch,
            _ => false,
        }
    }

    fn test_ahead2(&mut self, c1: char, c2: char) -> bool {
        self.test_ahead_chs(&[c1, c2])
    }

    fn advance(&mut self) {
        self.read();
    }

    fn advance2(&mut self) {
        self.read();
        self.read();
    }

    fn advance_n(&mut self, n: usize) {
        for _ in 0..n {
            self.read();
        }
    }

    fn skip_comment_single(&mut self) {
        self.advance2();
        loop {
            match self.read() {
                Some('\n') | None => break,
                _ => (),
            };
        }
    }

    fn skip_comment_multi(&mut self) {
        self.advance2();
        loop {
            match self.read() {
                Some('*') => {
                    if self.test_ahead('/') {
                        self.advance();
                        break;
                    }
                }
                None => break,
                _ => (),
            };
        }
    }

    fn test_ahead_chs(&mut self, chs: &[char]) -> bool {
        let mut pass = true;
        for i in 0..self.peeked.len() {
            pass = match self.peeked.get(i) {
                Some(c) => *c == chs[i],
                _ => false,
            };
            if !pass {
                return false;
            }
        }
        for i in self.peeked.len()..chs.len() {
            pass = match self.next_join_crlf() {
                Some(c) => {
                    self.peeked.push_back(c);
                    c == chs[i]
                }
                _ => false,
            };
            if !pass {
                return false;
            }
        }
        pass
    }

    fn ahead_is_id_part(&mut self) -> bool {
        match self.peek() {
            Some(c) => is_id_part(c),
            _ => false,
        }
    }

    fn ahead_is_eof(&mut self) -> bool {
        match self.peek() {
            Some(_) => false,
            _ => true,
        }
    }

    fn pos(&self) -> Position {
        Position {
            line: self.line,
            column: self.column,
        }
    }

    fn loc(&self) -> SourceLoc {
        SourceLoc {
            start: self.pos(),
            end: Position::new(),
        }
    }

    fn ahead_is_id_start(&mut self) -> bool {
        match self.peek() {
            Some(c) => is_id_start(c),
            _ => false,
        }
    }
}

fn is_line_terminator(c: char) -> bool {
    let cc = c as u32;
    cc == 0x0a || cc == 0x0d || cc == 0x2028 || cc == 0x2029
}

fn is_id_start(c: char) -> bool {
    if is_unicode_letter(c) {
        return true;
    }
    match c {
        '$' | '_' | '\\' => true,
        _ => false,
    }
}

fn is_id_part(c: char) -> bool {
    if is_id_start(c) {
        return true;
    }
    let cc = c as u32;
    if cc == 0x200c || cc == 0x200d {
        return true;
    }
    match GeneralCategory::of(c) {
        GeneralCategory::NonspacingMark
        | GeneralCategory::SpacingMark
        | GeneralCategory::DecimalNumber
        | GeneralCategory::ConnectorPunctuation => {
            true
        }
        _ => false,
    }
}

fn is_unicode_letter(c: char) -> bool {
    if c.is_uppercase() || c.is_lowercase() {
        return true;
    }
    match GeneralCategory::of(c) {
        GeneralCategory::TitlecaseLetter
        | GeneralCategory::ModifierLetter
        | GeneralCategory::OtherLetter
        | GeneralCategory::LetterNumber => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn expr_stmt_test() {
        let src = "a=1\n + 2;";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);

        let src = "set(a, 1, \n2, 10)";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);
        let stmt = &prog.body[0];
        match stmt {
            super::Stmt::CallExpr(expr_stmt) => {
                assert_eq!(
                    expr_stmt.expr,
                    "set(a, 1, 2, 10)".parse().unwrap()
                );
            }
            _ => panic!(),
        }
        let src = "//test test\n1 + 2;";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);
    }
    #[test]
    fn block_stmt_test() {
        let src = "/*test test\n*/{ }";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);
        let stmt = &prog.body[0];
        match stmt {
            super::Stmt::Block(block_stmt) => {
                assert_eq!(block_stmt.body.len(), 0);
            }
            _ => panic!(),
        }
    }
    #[test]
    fn if_stmt_test() {
        let src = "if 1 {}";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);
        let stmt = &prog.body[0];
        match stmt {
            super::Stmt::If(if_stmt) => {
                assert_eq!(if_stmt.test, "1".parse().unwrap());
            }
            _ => panic!(),
        }

        let src = "if a > b { return a + 1; }";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);

        let src = "if a > b { return a + 1; } else { return a *2; }";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);
    }

    #[test]
    fn for_stmt_test() {
        let src = "for i= 1; i < 10; i + 1 {}";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);
        let stmt = &prog.body[0];
        match stmt {
            super::Stmt::For(for_stmt) => {
                assert_eq!(for_stmt.test.clone().unwrap(), "i < 10".parse().unwrap());
                assert_eq!(for_stmt.update.clone().unwrap(), "i + 1".parse().unwrap());
            }
            _ => panic!(),
        }

        let src = "for ;; {continue;}";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);

        let src = "for ;a>b; {continue;}";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);

        let src = "for i = 0; i < 10;  {continue;}";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);

        let src = "for ;a>b; set(a,a+1) {continue;}";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);

        let src = "for i in 0..2 {continue;}";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);

        let src = "for i in 0..=2 {continue;}";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);
    }

    #[test]
    fn while_stmt_test() {
        let src = "while a > b {continue; break;} return;";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 2);
    }

    #[test]
    fn loop_stmt_test() {
        let src = "loop {continue; break;} return;";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 2);
    }
    #[test]
    fn fn_stmt_test() {
        let src = "fn f(a,b,c) {return 1;}";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);

        let src = "fn f(a,b,c) {return a+b+c;} f(1,2,3);";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 2);

        let src = "fn f(a,b,c) {return a+b+c;} f([],[],[]);";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 2);
    }

    #[test]
    fn call_stmt_test() {
        let src = " print(`abc`);";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);
        
        let src = "a = my_fun(\"a b c...\");";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);

        let src = "fn f(a,b,c) {return 1;} a = f(1,2,3);";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 2);

        let src = "a = native_f(\"./matrix.csv\");";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);

        let src = "native_f(\"./matrix1.csv\", \"./matrix2.csv\");";
        let mut parser = super::Parser::new(src);
        let prog = parser.prog().unwrap();
        assert_eq!(prog.body.len(), 1);
    }
}

pub fn load_prog(src: &str) -> Result<Prog, ParseError> {
    let mut code = String::new();
    let mut row = 0;
    for l in src.lines() {
        row += 1;
        let line = l.trim();
        if line.starts_with("#include_url") {
            let mut parts = line.split_whitespace();
            parts.next();
            if let Some(url) = parts.next() {
                let bytes= query_url(url).ok_or(ParseError::UnexpectedToken(row, 0))?;
                match String::from_utf8(bytes) {
                    Ok(s) => {
                        code += &s;
                    }
                    Err(e) => {
                        log::warn!("!!Failed to load utf8 from {url}: {e:?}");
                        return Err(ParseError::UnexpectedToken(row, 0));
                    }
                }
            }
        } else if line.starts_with("#include") {
            let mut parts = line.split_whitespace();
            parts.next();
            if let Some(file) = parts.next() {
                let path = Path::new(file);
                match fs::read_to_string(path) {
                    Ok(src) => {
                        code += &src;
                    }
                    Err(e) => {
                        log::warn!("!!Failed to read file {file}: {e:?}");
                        return Err(ParseError::UnexpectedToken(row, 0));
                    }
                }
            }
        } else {
            code += l;
            code += "\n";
        }
    } 
    parse_prog(&code)
}

pub(crate) fn query_url(url: &str) -> Option<Vec<u8>> {
    match reqwest::blocking::get(url) {
        Ok(resp) => {
            match resp.bytes() {
                Ok(bytes) => Some(bytes.to_vec()),
                Err(e) => {
                    log::warn!("!!Failed to load data from url {url}, err: {e}");
                    None
                }
            }
        }
        Err(e) => {
            log::warn!("!!Failed to load data from url {url}, err: {e}");
            None
        }
    }
}

pub fn parse_prog(code: &str) -> Result<Prog, ParseError> {
    let mut parser = Parser::new(code);
    parser.prog()
}

use petgraph::prelude::*;
pub fn create_stmt_tree(prog: &Prog) -> DiGraph<StmtNode, u32> {
    let mut g = DiGraph::new();
    let root = g.add_node(StmtNode::Root);
    for stmt in &prog.body {
       create_node(&mut g, root, stmt);
    }
    g
}

pub const PRE_TREAT_SELECT: &str = "pre_treat_";
fn create_node(g: &mut DiGraph<StmtNode, u32>, father: NodeIndex, stmt: &Stmt) {
    match stmt {
        Stmt::VarDec(s) => {
            // 如果是预处理的变量，不加入到树中
            if s.id.starts_with(PRE_TREAT_SELECT) {
                g.add_node(StmtNode::VarDec(s.as_ref().clone()));
                return;
            }
            let son = g.add_node(StmtNode::VarDec(s.as_ref().clone()));
            g.add_edge(father, son, g.edge_count() as u32 + 1);
        }
        Stmt::CallExpr(s) => {
            let son = g.add_node(StmtNode::CallExpr(s.as_ref().clone()));
            g.add_edge(father, son, g.edge_count() as u32 + 1);
        }
        Stmt::Break(s) => {
            let son = g.add_node(StmtNode::Break(s.as_ref().clone()));
            g.add_edge(father, son, g.edge_count() as u32 + 1);
        }
        Stmt::Continue(s) => {
            let son = g.add_node(StmtNode::Continue(s.as_ref().clone()));
            g.add_edge(father, son, g.edge_count() as u32 + 1);
        }
        Stmt::Return(s) => {
            let son = g.add_node(StmtNode::Return(s.as_ref().clone()));
            g.add_edge(father, son, g.edge_count() as u32 + 1);
        }
        Stmt::Block(s) => {
            let son = g.add_node(StmtNode::Block(BlockNode {
                loc: s.loc,
            }));
            g.add_edge(father, son, g.edge_count() as u32 + 1);
            for stmt in &s.body {
                create_node(g, son, stmt);
            }
        }
        Stmt::If(s) => {
            let son = g.add_node(StmtNode::If(IfNode {
                loc: s.loc,
                test: s.test.clone(),
            }));
            g.add_edge(father, son, g.edge_count() as u32 + 1);
            create_node(g, son, &s.body);
            if let Some(else_stmt) = &s.alt {
                let n = g.add_node(StmtNode::Else);
                g.add_edge(son, n, g.edge_count() as u32 + 1);
                create_node(g, n, else_stmt);
            }
        }
        Stmt::For(s) => {
            let son = g.add_node(StmtNode::For(ForNode {
                loc: s.loc,
                init: s.init.clone(),
                test: s.test.clone(),
                update: s.update.clone(),
            }));
            g.add_edge(father, son, g.edge_count() as u32 + 1);
            create_node(g, son, &s.body);
        }
        Stmt::While(s) => {
            let son = g.add_node(StmtNode::While(WhileNode {
                loc: s.loc,
                test: s.test.clone(),
            }));
            g.add_edge(father, son, g.edge_count() as u32 + 1);
            create_node(g, son, &s.body);
        }
        Stmt::Loop(s) => {
            let son = g.add_node(StmtNode::Loop(LoopNode {
                loc: s.loc,
            }));
            g.add_edge(father, son, g.edge_count() as u32 + 1);
            create_node(g, son, &s.body);
        }
        Stmt::Function(s) => {
            let son = g.add_node(StmtNode::Function(FnDecNode {
                loc: s.loc,
                id:  s.id.clone(),
                params: s.params.clone(),
            }));
            g.add_edge(father, son, g.edge_count() as u32 + 1);
            create_node(g, son, &s.body);
        }
        // not implemented
        Stmt::ForIn(_) => {}
    }
}