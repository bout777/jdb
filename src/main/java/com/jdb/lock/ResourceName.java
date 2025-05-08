package com.jdb.lock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ResourceName {
    private final List<String> names;

    public ResourceName(String name) {
        this(Collections.singletonList(name));
    }

    private ResourceName(List<String> names) {
        this.names = new ArrayList<>(names);
    }

    /**
     * @param parent This resource's parent, or null if this resource has no parent
     * @param name   The name of this resource.
     */
    ResourceName(ResourceName parent, String name) {
        this.names = new ArrayList<>(parent.names);
        this.names.add(name);
    }

    /**
     * @return null if this resource has no parent, a copy of this resource's
     * parent ResourceName otherwise.
     */
    ResourceName parent() {
        if (names.size() > 1) {
            return new ResourceName(names.subList(0, names.size() - 1));
        }
        return null;
    }

    /**
     * @return true if this resource is a descendant of `other`, false otherwise
     */
    boolean isDescendantOf(ResourceName other) {
        if (other.names.size() >= names.size()) {
            return false;
        }
        Iterator<String> mine = names.iterator();
        Iterator<String> others = other.names.iterator();
        while (others.hasNext()) {
            if (!mine.next().equals(others.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return this resource's names, e.g. a list like the following:
     * - ["database, "someTable", "10"]
     */
    List<String> getNames() {
        return names;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof ResourceName other)) return false;
        if (other.names.size() != this.names.size()) return false;
        for (int i = 0; i < other.names.size(); i++) {
            if (!this.names.get(i).equals(other.names.get(i))) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return names.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder rn = new StringBuilder(names.get(0));
        for (int i = 1; i < names.size(); ++i) {
            rn.append('/').append(names.get(i));
        }
        return rn.toString();
    }
}
