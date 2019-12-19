package de.tum.i13.server.kv.query;

public final class QueryBuilder {

    private final QueryType type;
    private String key;
    private String value;

    public enum QueryType {
        GET() {
            @Override
            public Query build() {
                return new GetQuery();
            }
        },
        PUT() {
            @Override
            public Query build() {
                return new PutQuery();
            }
        },
        DELETE() {
            @Override
            public Query build() {
                return new DeleteQuery();
            }
        };

        public abstract Query build();
    }

    public QueryBuilder(QueryType type) {
        this.type = type;
    }

    public static QueryBuilder put() {
        return new QueryBuilder(QueryType.PUT);
    }

    public static QueryBuilder get() {
        return new QueryBuilder(QueryType.GET);
    }

    public static QueryBuilder delete() {
        return new QueryBuilder(QueryType.DELETE);
    }

    public QueryBuilder key(String key) {
        this.key = key;
        return this;
    }

    public QueryBuilder value(String value) {
        this.value = value;
        return this;
    }

    public Query build() {
        return this.type.build().setKey(key).setValue(value);
    }
}
