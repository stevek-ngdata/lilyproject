/*
 * Copyright 2013 NGDATA nv
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
