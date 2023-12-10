package kv_interfaces;

public class PGSQLTemplates {
    private static String UPSERT_TEMPLATE = "";
    private static String DELETE_TEMPLATE = "";
    private static String READ_TEMPLATE = "SELECT value FROM %s WHERE ID = ?";
    private static String RANGE_TEMPLATE = "";

    public static String getUpsertTemplate(Object key) {
        return "INSERT INTO %s (ID, value) VALUES (" +
                (key instanceof Long ? "%d" : "'%s'") + ", '%s') ON CONFLICT (ID) DO UPDATE SET value = '%s'";
    }

    public static String getDeleteTemplate(Object key) {
        return DELETE_TEMPLATE;
    }

    public static String getReadTemplate(Object key) {
        return READ_TEMPLATE;
    }

    public static String getRangeTemplate(Object key) {
        return RANGE_TEMPLATE;
    }
}
