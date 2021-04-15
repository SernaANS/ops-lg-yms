package com.frubana.operations.logistics.yms.yard.application;

import com.frubana.operations.logistics.yms.common.configuration.FormattedLogger;
import com.frubana.operations.logistics.yms.common.utils.JsonUtils;
import com.frubana.operations.logistics.yms.yard.domain.Yard;
import com.frubana.operations.logistics.yms.yard.service.YardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.springframework.http.ResponseEntity.status;

/** Entry point for Some Controller.
 */
@RestController
@RequestMapping("/yms/yard")
public class YardController {

    /** Logger. */
    private final Logger logger =
            LoggerFactory.getLogger(YardController.class);

    /** Formatter to set the log in a specific format and add the body as part
     * of the same log. */
    private final FormattedLogger logFormatter;


    /** The jackson's object mapper, it's never null. */
    private final YardService yardService;



    /** Creates a new instance of the controller.
     *
     * @param yardService   The service used to process the requests,
     *                         required.
     * @param logFormatter     The formatter utility to log errors, required.
     */
    @Autowired
    public YardController(YardService yardService,
                          FormattedLogger logFormatter) {
        this.yardService = yardService;
        this.logFormatter = logFormatter;

    }

    /**
     * Return true if the service is healthy, otherwise false.
     * @return A 200 Status Code if the service is healthy
     */

    @GetMapping(
            value = "/healthz",
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public boolean healthCheck(){
        return yardService.isServiceHealthy();
    }


    /** Returns the task of the given id.
     *
     * @param id        The id of the task to request.
     * @param warehouse The warehouse where the task belongs.
     * @return A JSON representing a some object:
     * <code>
     * {@link Yard}
     * </code>
     */
    @GetMapping(
            value =  "/{warehouse}/{id}",
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Object> getYardInWarehouseById(
            @PathVariable(value = "warehouse") String warehouse,
            @PathVariable(value = "id") String id) {
        //Logging the given info
        HashMap<String, Object> params = new HashMap<>();
        params.put("id", id);
        params.put("warehouse", warehouse);
        logFormatter.logInfo(logger, "getYard", "Received request", params);

        if (id == null || id.isBlank()) {
            return status(HttpStatus.BAD_REQUEST).body(
                    JsonUtils.jsonResponse(HttpStatus.BAD_REQUEST,
                            "The id cannot be null or empty"));
        }
        if (warehouse == null || warehouse.isBlank()) {
            return status(HttpStatus.BAD_REQUEST).body(
                    JsonUtils.jsonResponse(HttpStatus.BAD_REQUEST,
                            "The warehouse cannot be null or empty"));
        }

        if (yardService.exists(id, warehouse)) {
            // Register the yard throws an error if something fails.
            Yard yard = yardService.getYard(id, warehouse);
            params.put("yard", yard);
            logFormatter.logInfo(logger, "obtainAYard", "found the Yard",
                    params);
            if(yard != null)
                return status(HttpStatus.OK).body(yard);
            else
                return status(HttpStatus.NOT_FOUND).body("Yard not Found");
        }

        return status(HttpStatus.NO_CONTENT).body(null);
    }


    /** Returns the yards of the given warehouse.
     *
     * @param warehouse The warehouse where the task belongs.
     * @return A JSON representing a some object:
     * <code>
     * {@link Yard}
     * </code>
     */
    @GetMapping(
            value =  "/{warehouse}/",
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Object> getAllYardsInWarehouse(
            @PathVariable(value = "warehouse") String warehouse) {
        //Logging the given info
        HashMap<String, Object> params = new HashMap<>();
        params.put("warehouse", warehouse);
        logFormatter.logInfo(logger, "getAllYardsInWarehouse",
                "Received request", params);
        if (warehouse == null || warehouse.isBlank()) {
            return status(HttpStatus.BAD_REQUEST).body(
                    JsonUtils.jsonResponse(HttpStatus.BAD_REQUEST,
                            "The warehouse cannot be null or empty"));
        }

        // Register the yard throws an error if something fails.
        List<Yard> yards = yardService.getYards(warehouse);
        params.put("yards", yards);
        HashMap<String,List<Yard>> yardsByWhs= new HashMap<>();
        for(Yard yard : yards) {
            if(!yardsByWhs.containsKey(yard.getColor())){
                yardsByWhs.put(yard.getColor(), new ArrayList<>());
            }
            List currentYards = yardsByWhs.get(yard.getColor());
            currentYards.add(yard);
            yardsByWhs.put(yard.getColor(), currentYards);
        }

        logFormatter.logInfo(logger, "obtainAYard", "found the Yard",
                params);
        if(yards != null)
            return status(HttpStatus.OK).body(yardsByWhs);
        else
            return status(HttpStatus.NOT_FOUND).body("Yard not Found");
    }

    /** Returns the yard of the given id.
     *
     * @return A JSON representing a some object:
     * <code>
     * {@link HashMap}<{@link String} warehouse,
     *                 {@link List}<{@link Yard}>
     *                >
     * </code>
     */
    @GetMapping(
            value =  "/",
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Object> getAllYardsByWarehouse() {
        //Logging the given info
        HashMap<String, Object> params = new HashMap<>();
        logFormatter.logInfo(logger, "getAllYardsByWarehouse",
                "Received request", params);
        // Register the yard throws an error if something fails.
        List<Yard> yards = yardService.getYards();
        params.put("yards", yards);
        HashMap<String,List<Yard>> yardsByWhs= new HashMap<>();
        for (Yard yard:yards) {
            if( !yardsByWhs.containsKey(yard.getWarehouse())) {
                yardsByWhs.put(yard.getWarehouse(), new ArrayList<>());
            }
            List<Yard> currentYards=yardsByWhs.get(yard.getWarehouse());
            currentYards.add(yard);
            yardsByWhs.put(yard.getWarehouse(),currentYards);
        }


        logFormatter.logInfo(logger, "obtainAYard", "found the Yard",
                params);
        if(!yards.isEmpty())
            return status(HttpStatus.OK).body(yardsByWhs);
        else
            return status(HttpStatus.NOT_FOUND).body("Yard not Found");
    }

    /** Generates the yard.
     *
     * @param yard the yard object to be persisted in the repository, cannot be
     *             null.
     * @return A JSON response with a message and status:
     * <code>
     * {
     * "message": "created",
     * "status": 201
     * }
     * </code>
     */
    @PostMapping(
            value = "/{warehouse}/",
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Object> register(
            @PathVariable(value = "warehouse") String warehouse,
            @RequestBody final Yard yard) {
        //Logging the given info

        HashMap<String, Object> params = new HashMap<>();
        params.put("yard", yard);
        params.put("warehouse", warehouse);
        logFormatter.logInfo(logger, "registerYard",
                "Received request", params);
        if (yard == null) {
            return status(HttpStatus.BAD_REQUEST).body(
                    JsonUtils.jsonResponse(HttpStatus.BAD_REQUEST,
                            "The Yard cannot be null"));
        }
        return status(HttpStatus.CREATED).body(
                yardService.registerYard(yard,warehouse)
        );


    }
    
     /** Generates the yard.
     *
     * @param yard the yard object to be persisted in the repository, cannot be
     *             null.
     * @return A JSON response with a message and status:
     * <code>
     * {
     * "message": "created",
     * "status": 201
     * }
     * </code>
     */
    @PostMapping(
            value = "/free/",
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Object> liberar(
            @RequestBody final Yard yard) {
        //Logging the given info

        HashMap<String, Object> params = new HashMap<>();
        params.put("yard", yard);
        logFormatter.logInfo(logger, "registerYard",
                "Received request", params);
        if (yard == null) {
            return status(HttpStatus.BAD_REQUEST).body(
                    JsonUtils.jsonResponse(HttpStatus.BAD_REQUEST,
                            "The Yard cannot be null"));
        }

        Yard yard2= yardService.liberar(yard);
        if(yard2 == null) { 
           return status(HttpStatus.BAD_REQUEST).body(
                    JsonUtils.jsonResponse(HttpStatus.BAD_REQUEST,
                                "yard no exist"));
        }
        return status(HttpStatus.CREATED).body(
                yard2
        );
    }

    /** Generates the yard.
     *
     * @param yard the yard object to be persisted in the repository, cannot be
     *             null.
     * @return A JSON response with a message and status:
     * <code>
     * {
     * "message": "created",
     * "status": 201
     * }
     * </code>
     */
    @PostMapping(
            value = "/occupy/",
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Object> occupy(
            @RequestBody final Yard yard) {
        //Logging the given info

        HashMap<String, Object> params = new HashMap<>();
        params.put("yard", yard);
        logFormatter.logInfo(logger, "registerYard",
                "Received request", params);
        if (yard == null) {
            return status(HttpStatus.BAD_REQUEST).body(
                    JsonUtils.jsonResponse(HttpStatus.BAD_REQUEST,
                            "The Yard cannot be null"));
        }

        Yard yard2= yardService.occupy(yard);
        if(yard2 == null) { 
           return status(HttpStatus.BAD_REQUEST).body(
                    JsonUtils.jsonResponse(HttpStatus.BAD_REQUEST,
                                "yard no exist"));
        }
        return status(HttpStatus.CREATED).body(
                yard2
        );
    }
 
}
