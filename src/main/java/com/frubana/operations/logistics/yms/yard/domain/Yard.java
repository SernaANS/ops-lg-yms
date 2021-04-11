package com.frubana.operations.logistics.yms.yard.domain;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;

/** Represents a some object minimum required data to generate a label.
 * <p> This is just a DTO that will be created by Jackson within the
 * application layer.
 * <p> This is an example of the expected JSON.
 * <code>
 * {
 *    "id":"TSK0041377",
 *    "taskStatus":"some status",
 *    "orderId":"OS000001",
 *    "orderIndex":123,
 *    "closeDate":"2020-05-22",
 *    "warehouse":"PA",
 *    "waveLabel":"OLA0000432",
 *    "waveType":"VENTA NORMAL",
 *    "sku":"SKU000001",
 *    "productName":"Onion",
 *    "unit":"KG",
 *    "quantity":300.33,
 *    "zoneId":"1",
 *    "route":"1-1",
 *    "etd":"14:30:30",
 *    "priority":1234567,
 *    "location":"MEH00321",
 *    "customerId":"andersoncalle148",
 *    "customerName":"Krasty Burgers",
 *    "source":"WMS"
 * }
 * </code>
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Yard {

    //a number that represents the unique id of the yard
    int id;
    //the color of this yard in hexadecimal representation #RRGGBB
    String color;
    //the number that represents the space location of this yard (NOT UNIQUE)
    int assignationNumber;
    // the String of the warehouse
    String warehouse;


    /**
     * Creates a new instance of the yard with all its attributes.
     * @param id the unique identifier of the Yard
     * @param color the color of the Yard
     * @param assignationNumber the space location of this yard.
     */
    public Yard(int id,
                String color,int assignationNumber){
        this.id=id;
        this.color=color;
        this.assignationNumber=assignationNumber;
        this.validate();
    }

    /**
     * Internal Validation of the instance of the object.
     * @return True if the {@link Yard} is Valid
     */
    private boolean validate()
    {
        if(!(this.id >0)){
            throw new IllegalArgumentException();
        }
        if(StringUtils.isEmpty(this.color) || StringUtils.isBlank(this.color)){
            throw new IllegalArgumentException();
        }
        return  true;
    }

    /**
     * Used for be created from the JSON, don't use it in the code!.
     */
    public Yard() {
    }

    /*
     * get the id of the yard.
     * @return the id in hex format.
     */
    public int getId() {
        return this.id;
    }

    /*
     * set the id of the yard.
     * @return the id in hex format.
     */
    public void setId(int id) {
        this.id=id;
    }

    /***
     * get the color of the yard.
     * @return the color in hex format.
     */
    public String getColor() {
        return this.color;
    }


    /***
     * get the assignationNumber of the yard.
     * @return the assignationNumber in hex format.
     */
    public int getAssignationNumber() {
        return this.assignationNumber;
    }

    /**
     * retrieve the warehouse
     * @return {@link String} the warehouse
     */
    public String getWarehouse(){return  this.warehouse; }

    public void AssignWarehouse(String warehouse){
        this.warehouse=warehouse;
    }


}