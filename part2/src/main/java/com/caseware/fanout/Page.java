package com.caseware.fanout;

import java.util.List;

public record Page(List<EngagementRef> items, String nextCursor) {

    public Page {
        items = List.copyOf(items);
    }

    public boolean hasNext() {
        return nextCursor != null;
    }
}
