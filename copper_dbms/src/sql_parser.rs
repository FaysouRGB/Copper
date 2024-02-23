pub enum SqlStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
}

pub struct SelectStatement {
    pub columns: Vec<String>,
    pub conditions: Option<Condition>,
}

pub struct InsertStatement {
    pub values: Vec<(String, SqlValue)>,
}

pub struct DeleteStatement {
    pub conditions: Option<Condition>,
}

pub enum Condition {
    Equal(String, SqlValue),
}

pub enum SqlValue {
    Int(i64),
    Str(String),
}

pub fn parse_sql(input: &str) -> Result<SqlStatement, String> {}
