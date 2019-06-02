import algoliasearch from "algoliasearch/lite";
import { InstantSearch, SearchBox, Hits } from "react-instantsearch-dom";
import * as React from "react";

const searchClient = algoliasearch("Q9QS39IFO0", "153e21a16f649c7f9ec28185683415cf");

export class Search extends React.Component<{}, {}> {
  public render() {
    return (
      <InstantSearch searchClient={searchClient} indexName="dataform_docs">
        <SearchBox />
        <Hits />
      </InstantSearch>
    );
  }
}
