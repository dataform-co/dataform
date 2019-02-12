-- some inline comment

/*js
type("table");

throw Error('Error in multiline comment');
*/
--js var foo = () => "foo";
select 1 as ${foo()}

/*
some multiline comment
*/
