import { Button, MenuItem } from "@blueprintjs/core";
import { IItemRendererProps, Omnibar } from "@blueprintjs/select";
import algoliasearch from "algoliasearch/lite";
import * as React from "react";
import {
  connectHits,
  createConnector,
  Hits,
  InstantSearch,
  SearchBox
} from "react-instantsearch-dom";
import { styles } from "../struct";

const searchClient = algoliasearch("Q9QS39IFO0", "153e21a16f649c7f9ec28185683415cf");

export class Search extends React.Component<{}, {}> {
  public render() {
    return (
      <InstantSearch searchClient={searchClient} indexName="dataform_docs">
        <CustomOmni />
      </InstantSearch>
    );
  }
}

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

interface IAlgoliaProps {
  refine: (value: string) => void;
  hits: IResult[];
}
interface IResult {
  hierarchy: { [key: string]: string };
  url: string;
}

interface IWidgetState {
  isOpen?: boolean;
  selectedItem?: boolean;
}

const SearchOmnibar = Omnibar.ofType<IResult>();

class SearchWidget extends React.Component<IAlgoliaProps, IWidgetState> {
  public state: IWidgetState = {};

  public render() {
    return (
      <div>
        <Button text="Search..." onClick={() => this.setState({ isOpen: true })} />
        <SearchOmnibar
          onQueryChange={query => this.props.refine(query)}
          onItemSelect={item => {
            console.log(item);
            this.setState({ isOpen: false });
            window.location.href = item.url;
          }}
          isOpen={this.state.isOpen}
          onActiveItemChange={item => console.log(item)}
          onClose={() => this.setState({ isOpen: false })}
          items={this.props.hits.filter(hit => !!hit.hierarchy)}
          itemRenderer={this.itemRenderer}
        />
      </div>
    );
  }

  public itemRenderer = (item: IResult, props: IItemRendererProps) => {
    const prettyString = ["lvl0", "lvl1", "lvl2", "lvl3"]
      .map(key => item.hierarchy[key])
      .filter(value => !!value)
      .join(" > ");
    return (
      <MenuItem
        text={<div className={"bp3-fill"}>{prettyString}</div>}
        active={props.modifiers.active}
      />
    );
  };
}

const CustomOmni = connectHits(connectWithQuery(SearchWidget));
