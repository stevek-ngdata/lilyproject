/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.repository.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * A hierarchical path value. This kind of value specifies a hierarchical path consisting of path segments.
 *
 * <p>See {@link ValueType}.
 *
 */
public class HierarchyPath implements List<Object>, Cloneable {

    private List<Object> elements;

    public HierarchyPath(Object... elements) {
        this.elements = new ArrayList<Object>(elements.length);
        Collections.addAll(this.elements, elements);
    }

    public Object[] getElements() {
        return toArray();
    }

    public int length() {
        return size();
    }

    @Override
    public Object clone() {
        return new HierarchyPath(getElements());
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public boolean isEmpty() {
        return elements.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return elements.contains(o);
    }

    @Override
    public Iterator<Object> iterator() {
        return elements.iterator();
    }

    @Override
    public Object[] toArray() {
        return elements.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return elements.toArray(a);
    }

    @Override
    public boolean add(Object o) {
        return elements.add(o);
    }

    @Override
    public boolean remove(Object o) {
        return elements.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return elements.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends Object> c) {
        return elements.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends Object> c) {
        return elements.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return elements.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return elements.retainAll(c);
    }

    @Override
    public void clear() {
        elements.clear();
    }

    @Override
    public boolean equals(Object o) {
        return elements.equals(o);
    }

    @Override
    public int hashCode() {
        return elements.hashCode();
    }

    @Override
    public Object get(int index) {
        return elements.get(index);
    }

    @Override
    public Object set(int index, Object element) {
        return elements.set(index, element);
    }

    @Override
    public void add(int index, Object element) {
        elements.add(index, element);
    }

    @Override
    public Object remove(int index) {
        return elements.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return elements.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return elements.lastIndexOf(o);
    }

    @Override
    public ListIterator<Object> listIterator() {
        return elements.listIterator();
    }

    @Override
    public ListIterator<Object> listIterator(int index) {
        return elements.listIterator(index);
    }

    @Override
    public List<Object> subList(int fromIndex, int toIndex) {
        return elements.subList(fromIndex, toIndex);
    }
}
