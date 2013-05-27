/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.util.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.junit.Test;

public class RepoAndTableUtilTest {
    
    @Test
    public void testBelongsToRepository_True() {
        assertTrue(RepoAndTableUtil.belongsToRepository("myRepo__myTableName", "myRepo"));
    }
    
    @Test
    public void testBelongsToRepository_False() {
        assertFalse(RepoAndTableUtil.belongsToRepository("myRepo__myTableName", "notMyRepo"));
    }
    
    @Test
    public void testIsValidTableName_True() {
        assertTrue(RepoAndTableUtil.isValidTableName("valid_table_name"));
    }
    
    @Test
    public void testIsValidTableName_False_WithInvalidChars() {
        assertFalse(RepoAndTableUtil.isValidTableName("invalid table"));
    }
    
    @Test
    public void testIsValidTableName_False_WithDoubleUnderscores() {
        assertFalse(RepoAndTableUtil.isValidTableName("invalid__table"));
    }
    
    @Test
    public void testSetRepositoryOwnership() {
        String repoName = "MyRepositoryName";
        HTableDescriptor tableDescriptor = new HTableDescriptor("MyTable");
        
        RepoAndTableUtil.setRepositoryOwnership(tableDescriptor, repoName);
        
        assertEquals(repoName, tableDescriptor.getValue(RepoAndTableUtil.OWNING_REPOSITORY_KEY));
    }
    
    @Test(expected=IllegalStateException.class)
    public void testRepositoryOwnership_TableDescriptorAlreadyContainsOwner() {
        HTableDescriptor tableDescriptor = new HTableDescriptor("MyTable");
        tableDescriptor.setValue(RepoAndTableUtil.OWNING_REPOSITORY_KEY, "MyRepositoryName");
        
        RepoAndTableUtil.setRepositoryOwnership(tableDescriptor, "AnotherRepositoryName");
    }

    @Test
    public void testGetOwningRepository() {
        HTableDescriptor tableDescriptor = new HTableDescriptor("MyTable");
        tableDescriptor.setValue(RepoAndTableUtil.OWNING_REPOSITORY_KEY, "MyRepositoryName");
        
        assertEquals("MyRepositoryName", RepoAndTableUtil.getOwningRepository(tableDescriptor));
    }
    
    @Test
    public void testGetOwningRepository_NoOwningRepository() {
        HTableDescriptor tableDescriptor = new HTableDescriptor("MyTable");
        
        assertNull(RepoAndTableUtil.getOwningRepository(tableDescriptor));
    }

}
