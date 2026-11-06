package com.caseware.fanout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class InMemoryEngagementCatalog implements EngagementCatalog {

    private final List<List<EngagementRef>> pages;
    private final List<String> cursors = Collections.synchronizedList(new ArrayList<>());

    InMemoryEngagementCatalog(List<List<EngagementRef>> pages) {
        this.pages = pages;
    }

    @Override
    public Page findEngagementsUsingTemplate(String templateId, String region, String cursor, int limit) {
        cursors.add(cursor);
        int index = cursor == null ? 0 : Integer.parseInt(cursor);
        if (index >= pages.size()) {
            return new Page(List.of(), null);
        }
        List<EngagementRef> items = pages.get(index);
        String next = index + 1 < pages.size() ? String.valueOf(index + 1) : null;
        return new Page(items, next);
    }

    int calls() {
        return cursors.size();
    }

    List<String> cursors() {
        return new ArrayList<>(cursors);
    }
}
