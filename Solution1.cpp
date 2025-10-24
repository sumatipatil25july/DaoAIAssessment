#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <set>
#include <string>
#include <filesystem>
#include <pqxx/pqxx>
#include <pqxx/zview>

namespace fs = std::filesystem;
using pqxx::operator""_zv;

struct Region
{
    long long id;       // inspection_region.id
    long long group_id; // inspection_region.group_id
    double coord_x;     // inspection_region.coord_x
    double coord_y;     // inspection_region.coord_y
    int category;       // inspection_region.category
};

// Load data from 3 files
std::vector<Region> loadData(const fs::path& dataDir)
{

    fs::path pointsFile = dataDir / "points.txt";
    fs::path categoriesFile = dataDir / "categories.txt";
    fs::path groupsFile = dataDir / "groups.txt";

    if (!fs::exists(pointsFile)) throw std::runtime_error("Missing file: " + pointsFile.string());
    if (!fs::exists(categoriesFile)) throw std::runtime_error("Missing file: " + categoriesFile.string());
    if (!fs::exists(groupsFile)) throw std::runtime_error("Missing file: " + groupsFile.string());

    std::ifstream pf(pointsFile);
    std::ifstream cf(categoriesFile);
    std::ifstream gf(groupsFile);

    std::vector<Region> regions;
    std::string pointLine, catLine, groupLine;
    long long id_counter = 1;

    while (std::getline(pf, pointLine))
    {
        if (!std::getline(cf, catLine) || !std::getline(gf, groupLine))
        {
            throw std::runtime_error("Mismatch in number of lines between files.");
        }

        std::istringstream iss(pointLine);
        double x, y;
        if (!(iss >> x >> y))
        {
            throw std::runtime_error("Invalid format in points.txt.");
        }

        Region r;
        r.id = id_counter++;
        r.coord_x = x;
        r.coord_y = y;
        r.category = static_cast<int>(std::stod(catLine));
        r.group_id = std::stoll(groupLine);

        regions.push_back(r);
    }

    return regions;
}

// Create tables according to schema
void createTables(pqxx::connection& conn)
{
    pqxx::work txn(conn);

    txn.exec(R"(
        CREATE TABLE IF NOT EXISTS inspection_group (
            id BIGINT NOT NULL PRIMARY KEY
        )
    )");

    txn.exec(R"(
        CREATE TABLE IF NOT EXISTS inspection_region (
            id BIGINT NOT NULL PRIMARY KEY,
            group_id BIGINT
        )
    )");

    txn.exec(R"(
        ALTER TABLE inspection_region ADD COLUMN IF NOT EXISTS coord_x FLOAT
    )");
    txn.exec(R"(
        ALTER TABLE inspection_region ADD COLUMN IF NOT EXISTS coord_y FLOAT
    )");
    txn.exec(R"(
        ALTER TABLE inspection_region ADD COLUMN IF NOT EXISTS category INTEGER
    )");

    txn.commit();
}

// Insert unique groups
void insertGroups(pqxx::connection& conn, const std::vector<Region>& regions)
{
    std::set<long long> uniqueGroups;
    for (const auto& r : regions) uniqueGroups.insert(r.group_id);

    pqxx::work txn(conn);
    auto streamer = pqxx::stream_to::table(
        txn,
        pqxx::table_path{ "inspection_group" },
        { "id" }
    );

    for (auto gid : uniqueGroups) streamer << std::make_tuple(gid);
    streamer.complete();
    txn.commit();
}

// Insert all regions
void insertRegions(pqxx::connection& conn, const std::vector<Region>& regions)
{
    pqxx::work txn(conn);
    auto streamer = pqxx::stream_to::table(
        txn,
        pqxx::table_path{ "inspection_region" },
        { "id", "group_id", "coord_x", "coord_y", "category" }
    );

    for (const auto& r : regions)
    {
        streamer << std::make_tuple(r.id, r.group_id, r.coord_x, r.coord_y, r.category);
    }

    streamer.complete();
    txn.commit();
}

int main(int argc, char* argv[])
{

    std::string data_directory;

    // Parse arguments: look for "--data_directory" followed by the path
    for (int i = 1; i < argc - 1; ++i)
    {
        std::string arg = argv[i];
        if (arg == "--data_directory")
        {
            data_directory = argv[i + 1];
            break;
        }
    }

    if (data_directory.empty()) {
        std::cerr << " Missing required argument: --data_directory <path>\n";
        return 1;
    }

    std::cout << "Using data directory: " << data_directory << "\n";

    try
    {
        fs::path dataDir(data_directory);
        std::cout << "Loading data from: " << dataDir << std::endl;

        auto regions = loadData(dataDir);

        // Connect to PostgreSQL (default 'postgres' database user is postgres)
        pqxx::connection conn(
            "dbname=postgres user=postgres password=Test@1234 hostaddr=127.0.0.1 port=5432"
        );

        if (!conn.is_open()) throw std::runtime_error("Cannot connect to PostgreSQL.");

        // Create database if it does not exist
        try
        {
            pqxx::nontransaction ntx(conn);

            // Check if database exists
            pqxx::result r =
                ntx.exec("SELECT 1 FROM pg_database WHERE datname = $1"_zv, pqxx::params{ "inspection_db" });
            if (r.empty())
            {
                ntx.exec("CREATE DATABASE inspection_db");
                std::cout << "Database 'inspection_db' created.\n";
            }
            else
            {
                std::cout << "Database 'inspection_db' already exists.\n";
            }
        }
        catch (const pqxx::sql_error& e)
        {
            if (std::string(e.what()).find("already exists") != std::string::npos)
            {
                std::cout << "Database 'inspection_db' already exists.\n";
            }
            else throw;
        }

        conn.close();

        // Connect to the new database
        pqxx::connection dbConn(
            "dbname=inspection_db user=postgres password=Test@1234 hostaddr=127.0.0.1 port=5432"
        );

        // Create tables
        createTables(dbConn);

        // Insert data
        insertGroups(dbConn, regions);
        insertRegions(dbConn, regions);

        dbConn.close();
        std::cout << "Data loaded successfully!\n";

    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
