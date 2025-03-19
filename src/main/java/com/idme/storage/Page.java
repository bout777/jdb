package com.idme.storage;

import static com.idme.common.Constants.PAGE_SIZE;

public class Page {
    private byte[] data;
    private  boolean isDirty;

    public Page(){
        this.data = new byte[PAGE_SIZE];
    }
    public void setDirty(boolean dirty){
        this.isDirty = dirty;
    }

    public boolean isDirty(){
        return this.isDirty;
    }

    public byte[] getData(){
        return this.data;
    }


}
