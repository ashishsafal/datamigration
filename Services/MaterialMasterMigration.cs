using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class MaterialMasterMigration : MigrationService
{
    protected override string SelectQuery => "SELECT ITEMID, ITEMCODE, ITEMNAME, ITEMDESCRIPTION, UOMId, MaterialGroupId, ClientSAPId FROM TBL_ITEMMASTER";
    protected override string InsertQuery => @"INSERT INTO material_master (material_id, material_code, material_name, material_description, uom_id, material_group_id, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@material_id, @material_code, @material_name, @material_description, @uom_id, @material_group_id, @company_id, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public MaterialMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "Direct",           // material_id
            "Direct",           // material_code
            "Direct",           // material_name
            "Direct",           // material_description
            "FK",               // uom_id
            "FK",               // material_group_id
            "FK",               // company_id
            "Default: 0",       // created_by
            "Default: Now",     // created_date
            "Default: null",    // modified_by
            "Default: null",    // modified_date
            "Default: false",   // is_deleted
            "Default: null",    // deleted_by
            "Default: null"     // deleted_date
        };
    }

    public async Task<int> MigrateAsync()
    {
        using var sqlConn = GetSqlServerConnection();
        using var pgConn = GetPostgreSqlConnection();
        await sqlConn.OpenAsync();
        await pgConn.OpenAsync();

        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);

        int insertedCount = 0;
        while (await reader.ReadAsync())
        {
            // Skip records where any FK is 0 to avoid constraint violations
            if ((int)reader["UOMId"] == 0 || (int)reader["MaterialGroupId"] == 0) continue;

            pgCmd.Parameters.Clear();
            pgCmd.Parameters.AddWithValue("@material_id", reader["ITEMID"]);
            pgCmd.Parameters.AddWithValue("@material_code", reader["ITEMCODE"]);
            pgCmd.Parameters.AddWithValue("@material_name", reader["ITEMNAME"]);
            pgCmd.Parameters.AddWithValue("@material_description", reader["ITEMDESCRIPTION"]);
            pgCmd.Parameters.AddWithValue("@uom_id", reader["UOMId"]);
            pgCmd.Parameters.AddWithValue("@material_group_id", reader["MaterialGroupId"]);
            pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
            pgCmd.Parameters.AddWithValue("@created_by", 0); // Default: 0
            pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow); // Default: Now
            pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@is_deleted", false); // Default: false
            pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value); // Default: null
            int result = await pgCmd.ExecuteNonQueryAsync();
            if (result > 0) insertedCount++;
        }
        return insertedCount;
    }
}