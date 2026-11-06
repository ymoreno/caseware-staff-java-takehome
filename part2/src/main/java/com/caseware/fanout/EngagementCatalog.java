package com.caseware.fanout;

public interface EngagementCatalog {

    Page findEngagementsUsingTemplate(String templateId, String region, String cursor, int limit);
}
