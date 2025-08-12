export type Tool = {
    id: string;
    name: string;
    url?: string | null;
    description?: string | null;
    tags?: string[] | null;
    categories?: string[] | null;
  };
  
  export type SearchResponse = {
    items: Tool[];
    q: string;
    limit: number;
    offset: number;
  };