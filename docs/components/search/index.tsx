import { Button, Menu, MenuItem } from "@blueprintjs/core";
import { IItemRendererProps, Omnibar } from "@blueprintjs/select";
import algoliasearch from "algoliasearch/lite";
import * as styles from "df/docs/components/search/index.css";
import * as React from "react";
import { connectHits, createConnector, InstantSearch } from "react-instantsearch-dom";

const searchClient = algoliasearch("Q9QS39IFO0", "153e21a16f649c7f9ec28185683415cf");

export class Search extends React.Component<{}, {}> {
  public render() {
    return (
      <InstantSearch searchClient={searchClient} indexName="dataform_docs">
        <ConnectedSearchWidget />
      </InstantSearch>
    );
  }
}

// Mostly magic to me: https://www.algolia.com/doc/guides/building-search-ui/widgets/create-your-own-widgets/react/
const connectWithQuery = createConnector({
  displayName: "WidgetWithQuery",
  getProvidedProps(props, searchState) {
    // Since the `attributeForMyQuery` searchState entry isn't
    // necessarily defined, we need to default its value.
    const currentRefinement = searchState.attributeForMyQuery || "";

    // Connect the underlying component with the `currentRefinement`
    return { currentRefinement };
  },
  refine(props, searchState, nextRefinement) {
    // When the underlying component calls its `refine` prop,
    // we update the searchState with the provided refinement.
    return {
      // `searchState` represents the search state of *all* widgets. We need to extend it
      // instead of replacing it, otherwise other widgets will lose their respective state.
      ...searchState,
      attributeForMyQuery: nextRefinement
    };
  },
  getSearchParameters(searchParameters, props, searchState) {
    // When the `attributeForMyQuery` state entry changes, we update the query
    return searchParameters.setQuery(searchState.attributeForMyQuery || "");
  },
  cleanUp(props, searchState) {
    // When the widget is unmounted, we omit the entry `attributeForMyQuery`
    // from the `searchState`, then on the next request the query will
    // be empty
    const { attributeForMyQuery, ...nextSearchState } = searchState;

    return nextSearchState;
  }
});

interface IResult {
  hierarchy: { [key: string]: string };
  url: string;
  content: string;
}

interface IWidgetProps {
  refine: (value: string) => void;
  hits: IResult[];
}

interface IWidgetState {
  isOpen?: boolean;
}

const SearchOmnibar = Omnibar.ofType<IResult>();

class SearchWidget extends React.Component<IWidgetProps, IWidgetState> {
  public state: IWidgetState = {};

  public render() {
    return (
      <>
        <Button icon="search" minimal={true} onClick={() => this.setState({ isOpen: true })} />
        <SearchOmnibar
          className={styles.omnibar}
          onQueryChange={query => this.props.refine(query)}
          onItemSelect={item => {
            this.setState({ isOpen: false });
            window.location.href = item.url;
          }}
          isOpen={this.state.isOpen}
          onClose={() => this.setState({ isOpen: false })}
          items={this.props.hits.filter(hit => !!hit.hierarchy)}
          itemRenderer={this.itemRenderer}
        />
      </>
    );
  }

  public itemRenderer = (item: IResult, props: IItemRendererProps) => {
    const prettyString = ["lvl0", "lvl1", "lvl2", "lvl3"]
      .map(key => item.hierarchy[key])
      .filter(value => !!value)
      .join(" > ");
    return (
      <div className={props.modifiers.active ? styles.hitActive : styles.hit}>
        <a href={item.url}>
          <span className={styles.hitHierarchy}>{prettyString}</span>
        </a>
        {item.content && (
          <div className={styles.hitContent} dangerouslySetInnerHTML={{ __html: item.content }} />
        )}
      </div>
    );
  };
}

const ConnectedSearchWidget = connectHits(connectWithQuery(SearchWidget));
