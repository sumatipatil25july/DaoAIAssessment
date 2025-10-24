#include <pqxx/pqxx>
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>

using json = nlohmann::json;
namespace fs = std::filesystem;

struct Point
{
    double x;
    double y;
    int category;
    long group_id;
};

// check if a point is inside a rectangle
bool isInside(double x, double y, double xmin, double ymin, double xmax, double ymax)
{
    return (x >= xmin && x <= xmax && y >= ymin && y <= ymax);
}

int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: --query=<path_to_json file>\n";
        return 1;
    }

    std::string query_file = argv[1];
    if (query_file.rfind("--query=", 0) == 0)
        query_file = query_file.substr(8);

    fs::path filePath(query_file);
    std::cout << "Taking Query from : " << query_file << std::endl;

    std::ifstream fin(filePath);
    if (!fin.is_open())
    {
        std::cerr << "Error: Cannot open JSON file " << query_file << std::endl;
        return 1;
    }

    json j;
    fin >> j;
    fin.close();

    // Extract valid region
    auto region = j["query"]["operator_crop"]["region"];
    double xmin = region["p_min"]["x"];
    double ymin = region["p_min"]["y"];
    double xmax = region["p_max"]["x"];
    double ymax = region["p_max"]["y"];

    bool has_category = j["query"]["operator_crop"].contains("category");
    bool has_groups = j["query"]["operator_crop"].contains("one_of_groups");
    bool is_proper = j["query"]["operator_crop"].value("proper", false);

    int category = has_category ? j["query"]["operator_crop"]["category"].get<int>() : -1;
    std::vector<long> one_of_groups;
    if (has_groups)
        one_of_groups = j["query"]["operator_crop"]["one_of_groups"].get<std::vector<long>>();

    try {
        pqxx::connection conn("dbname=inspection_db user=postgres password=Test@1234 host=localhost port=5432");
        pqxx::work txn(conn);

        // Build SQL query dynamically
        std::stringstream query;
        query << "SELECT coord_x, coord_y, category, group_id "
            << "FROM inspection_region "
            << "WHERE coord_x BETWEEN " << xmin << " AND " << xmax
            << " AND coord_y BETWEEN " << ymin << " AND " << ymax;

        if (has_category)
            query << " AND category = " << category;

        if (has_groups)
        {
            query << " AND group_id IN (";
            for (size_t i = 0; i < one_of_groups.size(); ++i)
            {
                if (i > 0) query << ",";
                query << one_of_groups[i];
            }
            query << ")";
        }

        query << " ORDER BY coord_y, coord_x;";

        pqxx::result res = txn.exec(query.str());

        std::vector<Point> points;
        for (auto row : res)
        {
            points.push_back({
                row["coord_x"].as<double>(),
                row["coord_y"].as<double>(),
                row["category"].as<int>(),
                row["group_id"].as<long>()
                });
        }

        // filter for "proper" groups (only groups fully inside region)
        if (is_proper)
        {
            std::vector<long> valid_groups;
            std::map<long, bool> group_validity;

            for (auto& p : points)
                group_validity[p.group_id] = true;

            // Validate all points of every group
            pqxx::result all = txn.exec("SELECT group_id, coord_x, coord_y FROM inspection_region;");
            for (auto row : all)
            {
                long gid = row["group_id"].as<long>();
                double x = row["coord_x"].as<double>();
                double y = row["coord_y"].as<double>();
                if (!isInside(x, y, xmin, ymin, xmax, ymax))
                    group_validity[gid] = false;
            }

            // Keep only points with fully valid groups
            std::vector<Point> filtered;
            for (auto& p : points)
                if (group_validity[p.group_id])
                    filtered.push_back(p);

            points.swap(filtered);
        }

        // Write results to output file
        std::ofstream fout("query_output.txt");
        for (auto& p : points)
            fout << p.x << " " << p.y << " " << p.category << " " << p.group_id << "\n";
        fout.close();

        std::cout << "Query executed successfully. Results written to query_output.txt\n";
        txn.commit();
    }
    catch (const std::exception& e)
    {
        std::cerr << "Database error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
