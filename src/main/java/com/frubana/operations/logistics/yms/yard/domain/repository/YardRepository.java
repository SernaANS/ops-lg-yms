package com.frubana.operations.logistics.yms.yard.domain.repository;

import com.frubana.operations.logistics.yms.yard.domain.Yard;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.Query;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.statement.Update;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/** Some repository using JDBI
 */
@Component
public class YardRepository {
    
    /** The JDBI instance to request data to the database, it's never null. */
    private final Jdbi dbi;

    /** Base constructor of the repository.
     *
     * @param jdbi the JDBI instance to use in the queries.
     */
    @Autowired
    public YardRepository(Jdbi jdbi) {
        this.dbi = jdbi;
    }

    /**
     * register a yard for a specific warehouses.
     * @param yard the yard to be register.
     * @param warehouse the warehouse to be registered.
     * @return the {@link Yard}  registered.
     */
    public Yard register(Yard yard, String warehouse){
        int nextAssignation = this.getNextAssignationNumber(yard.getColor(),
                warehouse);
        String sql_query="Insert into yard (color, warehouse, assignation_number)"+
                " values(:color, :warehouse, :nextAssignation)";
        try(Handle handler=dbi.open();
            Update query_string = handler.createUpdate(sql_query)){
            query_string
                    .bind("color",yard.getColor())
                    .bind("warehouse",warehouse)
                    .bind("nextAssignation", nextAssignation);
            int yard_id=query_string
                    .executeAndReturnGeneratedKeys("id")
                    .mapTo(int.class).first();
            handler.close();
            Yard createdYard = new Yard(yard_id,yard.getColor(),
                    nextAssignation);
            createdYard.AssignWarehouse(warehouse);
            return createdYard ;
        }
    }
    
    /**
     * update a yard for a specific warehouses, assignationNumber and default_color.
     * @param yard the yard to be update.
     * @return the {@link Yard}  update.
     */
    public Yard updateColorYardFree(Yard yard){
        String sql_query="Update yard set color = default_color"+
                " WHERE color = :color and warehouse=:warehouse and assignation_number=:assignationNumber";
        try(Handle handler=dbi.open();
            Update query_string = handler.createUpdate(sql_query)){
            query_string
                    .bind("color",yard.getColor())
                    .bind("warehouse",yard.getWarehouse())
                    .bind("assignationNumber",yard.getAssignationNumber());
            int yard_id=query_string
                    .executeAndReturnGeneratedKeys("id")
                    .mapTo(int.class).first();
            yard.setId(yard_id);
            handler.close();
            return yard;
        }
    }

    /**
     * update yard color a occuppy 
     * @param yard the yard to be update.
     * @return the {@link Yard}  update.
     */
    public Yard updateColorYardOccupy(Yard yard){
        String sql_query="Update yard set color ='#D3D3D3'"+
        " WHERE  default_color = :color and warehouse=:warehouse and assignation_number=:assignationNumber";
        try(Handle handler=dbi.open();
            Update query_string = handler.createUpdate(sql_query)){
            query_string
                    .bind("color",yard.getColor())
                    .bind("warehouse",yard.getWarehouse())
                    .bind("assignationNumber",yard.getAssignationNumber());
            int yard_id=query_string
                    .executeAndReturnGeneratedKeys("id")
                    .mapTo(int.class).first();
            yard.setId(yard_id);
            handler.close();
            return yard;
        }
    }

    /**
     * @param color
     * @param warehouse
     * @return
     */
    private int getNextAssignationNumber(String color, String warehouse){
        String sql_query = "Select assignation_number from YARD " +
        "where color=:color and warehouse=:warehouse order by assignation_number ASC";

        try (Handle handler = dbi.open(); Query query_string = handler.createQuery(sql_query)) {
        	query_string
            	.bind("color", color)
            	.bind("warehouse", warehouse);
        	List<Integer> assignationNumbers = query_string.mapTo(Integer.class).list();
            handler.close();
            int assignationNumber = 0;
            for (Integer num : assignationNumbers) {
            	assignationNumber++;
				if (assignationNumber != num) {
					return assignationNumber;
				}
			}
            return assignationNumber+1;
        }
        
    }

    /**
     * Retrieve if an yard exists or not in the DB
     * @param id the id of the yard
     * @param warehouse the warehouse to be retrieved
     * @return the {@link Boolean} that checks if a yard exists
     */
    public boolean exist(int id, String warehouse) {
        String sql_query = "Select count(*) from YARD " +
                "where id= :id and warehouse=:warehouse";
        try (Handle handler = dbi.open();
             Query query_string = handler.createQuery(sql_query)) {
            query_string
                    .bind("id", id)
                    .bind("warehouse", warehouse);
            int yard_id = query_string.mapTo(int.class).first();
            handler.close();
            return yard_id > 0;
        }
    }

    /**
     * Retrieve a {@link Yard} by its id and warehouse.
     * @param id the id for yard
     * @param warehouse the warehouse that you are asking for.
     * @return the Yard if exist.
     */
    public Yard getByIdAndWarehouse(int id, String warehouse) {
        String sql_query = "Select id,color,warehouse,assignation_number "+
                "from YARD " +
                "where id= :id and warehouse=:warehouse";
        try (Handle handler = dbi.open();
             Query query_string = handler.createQuery(sql_query)) {
            query_string
                    .bind("id", id)
                    .bind("warehouse", warehouse);
            Yard yard = query_string.mapTo(Yard.class).first();
            handler.close();
            return yard;
        }
    }

    public List<Yard> getByWarehouse(String warehouse) {
        String sql_query = "Select id,color,warehouse,assignation_number "+
                "from YARD " +
                "where warehouse=:warehouse order by assignation_number";
        try (Handle handler = dbi.open();
             Query query_string = handler.createQuery(sql_query)) {
            query_string
                    .bind("warehouse", warehouse);
            List<Yard> yards = query_string.mapTo(Yard.class).list();
            handler.close();
            return yards;
        }
    }

    public Yard getByWarehouseAndAssignationNumber(String warehouse, int assignationNumber) {
        String sql_query = "Select id,color,warehouse,assignation_number "+
                "from YARD " +
                "where warehouse=:warehouse and assignation_number=:assignationNumber order by assignation_number";
        try (Handle handler = dbi.open();
             Query query_string = handler.createQuery(sql_query)) {
            query_string
                    .bind("warehouse", warehouse)
                    .bind("assignationNumber", assignationNumber);
            Yard yard = query_string.mapTo(Yard.class).first();
            handler.close();
            return yard;
        }
    }

    public List<Yard> getAll() {
        String sql_query = "Select id,color,warehouse,assignation_number "+
                "from YARD ";
        try (Handle handler = dbi.open();
             Query query_string = handler.createQuery(sql_query)) {
            List<Yard> yards = query_string.mapTo(Yard.class).list();
            handler.close();
            return yards;
        }
    }

    /** Mapper of the {@link Yard} for the JDBI implementation.
     */
    @Component
    public static class YardMapper implements RowMapper<Yard> {

        /** Override of the map method to set the fields in the SomeObject
         * object when extracted from the repository.
         *
         * @param rs  result set with the fields of the extracted some object.
         * @param ctx the context of the request that extracted the some
         *            object.
         * @return the {@link Yard} instance with the extracted fields.
         * @throws SQLException if the result set throws an error when
         *                      extracting some field.
         */
        @Override
        public Yard map(ResultSet rs, StatementContext ctx)
                throws SQLException {
            Yard yard = new Yard(
                    rs.getInt("id"),
                    rs.getString("color"),
                    rs.getInt("assignation_number")
            );
            yard.AssignWarehouse(rs.getString("warehouse"));
            return yard;
        }
    }
}
