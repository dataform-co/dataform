import * as React from "react";
import { PrismLight as SyntaxHighlighter } from "react-syntax-highlighter";
import sql from "react-syntax-highlighter/dist/cjs/languages/prism/sql";

// All themes available here https://github.com/conorhastings/react-syntax-highlighter/tree/master/src/styles/hljs.
// import dark from "react-syntax-highlighter/dist/cjs/styles/hljs/tomorrow-night";
// import vs from "react-syntax-highlighter/dist/cjs/styles/prism/vs";

SyntaxHighlighter.registerLanguage("sql", sql);

export function Code({ children }: React.PropsWithChildren<{}>) {
  return (
    <SyntaxHighlighter
      language="sql"
      showLineNumbers={true}
      wrapLines={true}
      useInlineStyles={false}
      codeTagProps={{ className: "language-sql" }}
    >
      {children}
    </SyntaxHighlighter>
  );
}
