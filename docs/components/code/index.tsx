import * as React from "react";
import { PrismLight as SyntaxHighlighter } from "react-syntax-highlighter";

import sql from "react-syntax-highlighter/dist/cjs/languages/prism/sql";

SyntaxHighlighter.registerLanguage("sql", sql);

export function Code({ fileName, children }: React.PropsWithChildren<{ fileName?: string }>) {
  return (
    <>
      {fileName && <div className={`hljs-filename`}>{fileName}</div>}
      <SyntaxHighlighter
        language="sql"
        showLineNumbers={true}
        wrapLines={true}
        useInlineStyles={false}
        codeTagProps={{ className: "language-sql" }}
      >
        {children}
      </SyntaxHighlighter>
    </>
  );
}
