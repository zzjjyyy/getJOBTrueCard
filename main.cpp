#include "duckdb.hpp"

using namespace duckdb;

#define CHECK_BIT(x, n) (x >> n)

typedef struct rel
{
	int relid;
	double rows;
} rel;

int find(int x, int* father, int length)
{
	int a;
	a = x;
	while (x != father[x])
	{
		x = father[x];
	}
	while (a != father[a])
	{
		int z = a;
		a = father[a];
		father[z] = x;
	}
	return x;
}

void Union(int a, int b, int* father, int length)
{
	int fx = find(a, father, length);
	int fy = find(b, father, length);
	if (fx != fy)
	{
		father[fx] = fy;
	}
}

// index is a member of bms
bool isMember(int bms, int index)
{
	if (bms < index)
		return false;
	else
	{
		int i = 0;
		while (index > 0)
		{
			if (index % 2 == 1)
			{
				int temp = bms >> i;
				if (temp % 2 == 0)
					return false;
			}
			index = index >> 1;
			i++;
		}
	}
	return true;
}

std::string generateSubquery(std::vector<std::string> From, std::vector<int> Whereindex, std::vector<std::string> Where, int bms)
{
	std::string res("SELECT * FROM ");
	std::vector<std::string> local_from;
	std::vector<int> local_fromidx;
	std::vector<std::string> local_where;
	std::vector<int> local_whereidx;
	int temp = bms;
	int a = 0;
	while (temp != 0)
	{
		int flag = temp % 2;
		if (flag == 1)
		{
			local_from.push_back(From[a]);
			local_fromidx.push_back((1 << a));
		}
		temp = temp >> 1;
		a++;
	}
	for (int i = 0; i < Whereindex.size(); i++)
	{
		if (isMember(bms, Whereindex[i]))
		{
			local_where.push_back(Where[i]);
			local_whereidx.push_back(Whereindex[i]);
		}
	}
	if (local_from.size() > 1)
	{
		int m, n, i, k = 0;
		int father[25];
		for (i = 0; i < local_from.size(); i++)
		{
			father[i] = i;
		}
		for (int i = 0; i < local_fromidx.size() - 1; i++)
		{
			for (int j = i + 1; j < local_fromidx.size(); j++)
			{
				for (int z = 0; z < local_whereidx.size(); z++)
				{
					if (local_fromidx[i] + local_fromidx[j] == local_whereidx[z])
					{
						Union(i, j, father, local_from.size());
					}
				}
			}
		}
		for (i = 0; i < local_from.size(); i++)
		{
			if (father[i] == i)
			{
				k++;
			}
		}
		if (k != 1)
			return "";
	}
	for (int i = 0; i < local_from.size(); i++)
	{
		res.append(local_from[i]);
		if (i < local_from.size() - 1)
			res.append(", ");
	}
	for (int i = 0; i < local_where.size(); i++)
	{
		if (i == 0)
			res.append(" WHERE ");
		res.append(local_where[i]);
		if (i < local_where.size() - 1)
			res.append(" AND ");
	}
	res.append(";\n");
	return res;
}

int main() {
	char fpath[100];
	sprintf(fpath, "./JoinOrderBenchmark/JOB.sql");
	FILE* f_query = fopen(fpath, "r");
	char line[4096] = "";
	int query_id = 1;
	while (fgets(line, 2048, f_query) != NULL)
	{
		if(line[0] == '\n')
			continue;
		char query_sql[32];
		sprintf(query_sql, "./answer/query%d.txt", query_id);
		std::cout << query_sql << std::endl;
		query_id = query_id + 1;
		if(query_id < 101) {
			continue;
		}
		std::vector<rel> records;
		std::vector<std::string> From;
		std::vector<std::string> Where;
		std::vector<int> Whereindex;
		std::string query_str(line);
		size_t pos = query_str.find("FROM ");
		std::string from_str(query_str);
		from_str = from_str.substr(pos + 5);
		pos = from_str.find("WHERE ");
		from_str = from_str.substr(0, pos);
		size_t offset = 0;
		std::string temp;
		//process from clause
		while ((pos = from_str.find(", ", offset)) != std::string::npos)
		{
			temp = from_str.substr(offset, pos - offset);
			if (temp.length() > 0)
			{
				From.push_back(temp);
			}
			offset = pos + 2;
		}
		temp = from_str.substr(offset);
		temp = temp.substr(0, temp.length() - 1);
		if (temp.length() > 0)
		{
			From.push_back(temp);
		}
		int length = From.size();
		//process where clause
		std::string where_str(query_str);
		pos = where_str.find("WHERE ");
		where_str = where_str.substr(pos + 6);
		offset = 0;
		//process where clause
		while ((pos = where_str.find(" AND ((", offset)) != std::string::npos)
		{
			size_t pos_head = pos;
			offset = pos + 5;
			pos = where_str.find(")) ", offset);
			temp = where_str.substr(offset, pos + 2 - offset);
			if (temp.length() > 0)
			{
				Where.push_back(temp);
			}
			where_str.erase(pos_head, pos - pos_head + 2);
			offset = 0;
		}
		while ((pos = where_str.find(" AND (", offset)) != std::string::npos)
		{
			size_t pos_head = pos;
			offset = pos + 5;
			pos = where_str.find(") ", offset);
			temp = where_str.substr(offset, pos + 1 - offset);
			if (temp.length() > 0)
			{
				Where.push_back(temp);
			}
			where_str.erase(pos_head, pos - pos_head + 1);
			offset = 0;
		}
		offset = 0;
		while ((pos = where_str.find(" AND ", offset)) != std::string::npos)
		{
			temp = where_str.substr(offset, pos - offset);
			if (temp.length() > 0)
			{
				Where.push_back(temp);
			}
			offset = pos + 5;
		}
		temp = where_str.substr(offset);
		temp = temp.substr(0, temp.length() - 2);
		if (temp.length() > 0)
		{
			Where.push_back(temp);
		}
		//create where index
		for (int a = 0; a < Where.size(); a++) {
			int index = 0;
			std::string str = Where[a];
			for (int b = 0; b < From.size(); b++) {
				std::string str1(From[b]);
				size_t temp_pos = str1.find(" AS");
				str1 = str1.substr(temp_pos + 4, str1.size()- temp_pos - 4);
				str1.append(".");
				size_t last_pos = 0;
				size_t f_pos = 0;
				while((f_pos = str.find(str1, last_pos)) != std::string::npos) {
					last_pos = f_pos + 1;
					/* 区别 mk 和 k */
					if ((f_pos == 0) || (str[f_pos - 1] == ' ' || str[f_pos - 1] == '('))
					{
						if((index >> b) % 2 == 0) {
							index = index + (1 << b);
						}
					}
				}
			}
			Whereindex.push_back(index);
		}
		//enurmerate subqueries
		std::vector<std::vector<int>> join_rel_level(length);
		for (int a = 0; a < length; a++)
		{
			int bms = 1 << a;
			bool flag;
			if (join_rel_level[0].size() > 0)
			{
				flag = false;
				for(int j = 0; j < join_rel_level[0].size(); j++)
				{
					int prev = join_rel_level[0][j];
					if (prev == bms)
					{
						flag = true;
						break;
					}
				}
				if (flag)
				{
					continue;
				}
			}
			join_rel_level[0].push_back(bms);
			std::string subquery = generateSubquery(From, Whereindex, Where, bms);
			bool found = false;
			char* pos;
			char* p;
			char cmp[1000];
			int relid_in_file;
			FILE* f_check = fopen(query_sql, "r+");
			if(f_check) {
				while(fgets(cmp, 1000, f_check) != NULL) {
					if((pos = strchr(cmp, '\n')) != NULL) {
						*pos = '\0';
					}
					p = strtok(cmp, ":");
					relid_in_file = atoi(p);
					if(relid_in_file == (bms << 1)) {
						found = true;
						break;
					}
				}
				fclose(f_check);
			}
			if(found) {
				continue;
			}
			std::cout << subquery;
			DuckDB db("../giveDuckTopDownOptimizer/JoinOrderBenchmark.db");
			Connection con(db);
			auto result = con.Query(subquery.c_str());
			rel r = { bms << 1, result->RowCount() };
			FILE* fp = fopen(query_sql, "a+");
			fprintf(fp, "%d:%lf\n", r.relid, r.rows);
			fclose(fp);
		}
		for (int a = 1; a < length; a++)
		{
			for(int j = 0; j < join_rel_level[a - 1].size(); j++)
			{	
				int bms = join_rel_level[a - 1][j];
				for (int k = 0; k < length; k++)
				{
					if (isMember(bms, 1 << k))
					{
						continue;
					}
					int new_bms = (1 << k) + bms;
					if (join_rel_level[a].size() > 0)
					{
						bool flag = false;
						for(int m = 0; m < join_rel_level[a].size(); m++)
						{
							int prev = join_rel_level[a][m];
							if (prev == new_bms)
							{
								flag = true;
								break;
							}
						}
						if (flag)
						{
							continue;
						}
					}
					join_rel_level[a].push_back(new_bms);
					std::string subquery = generateSubquery(From, Whereindex, Where, new_bms);
					if (subquery == "")
						continue;
					if ((query_id == 24 && (new_bms == 11 || new_bms == 25 || new_bms == 27))
					 || (query_id == 30 && (new_bms == 7 || new_bms == 15 || new_bms == 19 || new_bms == 22 || new_bms == 23 || new_bms == 31 || new_bms == 67 || new_bms == 71 || new_bms == 79 || new_bms == 82 || new_bms == 83 || new_bms == 86 || new_bms == 87 || new_bms == 94 || new_bms == 95))
					 || (query_id == 31 && (new_bms == 7 || new_bms == 15 || new_bms == 19 || new_bms == 22 || new_bms == 23 || new_bms == 31 || new_bms == 67 || new_bms == 71 || new_bms == 79 || new_bms == 82 || new_bms == 83 || new_bms == 86 || new_bms == 87 || new_bms == 94 || new_bms == 95))
					 || (query_id == 44 && (new_bms == 26 || new_bms == 30))
					 || (query_id == 46 && (new_bms == 26 || new_bms == 30 || new_bms == 154 || new_bms == 410))
					 || (query_id == 47 && (new_bms == 26 || new_bms == 30 || new_bms == 154 || new_bms == 410))
					 || (query_id == 48 && (new_bms == 26 || new_bms == 30 || new_bms == 154 || new_bms == 410))
					 || (query_id == 49 && (new_bms == 26 || new_bms == 30 || new_bms == 154 || new_bms == 410))
					 || (query_id == 57 && (new_bms == 3 || new_bms == 6 || new_bms == 7 || new_bms == 14 || new_bms == 15 || new_bms == 18 || new_bms == 19 || new_bms == 20 || new_bms == 22 || new_bms == 23 || new_bms == 30 || new_bms == 31 || new_bms == 66 || new_bms == 70 || new_bms == 71 || new_bms == 78 || new_bms == 79 || new_bms == 82 || new_bms == 83 || new_bms == 86 || new_bms == 87 || new_bms == 94 || new_bms == 95 || new_bms == 114 || new_bms == 118 || new_bms == 119 || new_bms == 126))
					 || (query_id == 58 && (new_bms == 3 || new_bms == 6 || new_bms == 7 || new_bms == 14 || new_bms == 15 || new_bms == 18 || new_bms == 19 || new_bms == 20 || new_bms == 22 || new_bms == 23 || new_bms == 28 || new_bms == 30 || new_bms == 31 || new_bms == 55 || new_bms == 66 || new_bms == 67 || new_bms == 70 || new_bms == 71 || new_bms == 78 || new_bms == 79 || new_bms == 82 || new_bms == 83 || new_bms == 86 || new_bms == 87 || new_bms == 94 || new_bms == 95 || new_bms == 114 || new_bms == 118 || new_bms == 119 || new_bms == 126 || new_bms == 130 || new_bms == 131 || new_bms == 134 || new_bms == 135 || new_bms == 140 || new_bms == 142 || new_bms == 143 || new_bms == 146 || new_bms == 147 || new_bms == 148 || new_bms == 150 || new_bms == 151 || new_bms == 156 || new_bms == 158 ||new_bms == 159 || new_bms == 194 || new_bms == 195 || new_bms == 198 || new_bms == 199 || new_bms == 206 || new_bms == 207 || new_bms == 210 || new_bms == 211 || new_bms == 214 || new_bms == 215 || new_bms == 222 || new_bms == 223 || new_bms == 242 || new_bms == 243 || new_bms == 246 || new_bms == 254))
					 || (query_id == 59 && (new_bms == 3 || new_bms == 6 || new_bms == 7 || new_bms == 14 || new_bms == 15 || new_bms == 18 || new_bms == 19 || new_bms == 20 || new_bms == 22 || new_bms == 23 || new_bms == 28 || new_bms == 30 || new_bms == 31 || new_bms == 55 || new_bms == 66 || new_bms == 67 || new_bms == 70 || new_bms == 71 || new_bms == 78 || new_bms == 79 || new_bms == 82 || new_bms == 83 || new_bms == 86 || new_bms == 87 || new_bms == 94 || new_bms == 95 || new_bms == 114 || new_bms == 118 || new_bms == 119 || new_bms == 126 || new_bms == 130 || new_bms == 131 || new_bms == 134 || new_bms == 135 || new_bms == 140 || new_bms == 142 || new_bms == 143 || new_bms == 146 || new_bms == 147 || new_bms == 148 || new_bms == 150 || new_bms == 151 || new_bms == 156 || new_bms == 158 ||new_bms == 159 || new_bms == 194 || new_bms == 195 || new_bms == 198 || new_bms == 199 || new_bms == 206 || new_bms == 207 || new_bms == 210 || new_bms == 211 || new_bms == 214 || new_bms == 215 || new_bms == 222 || new_bms == 223 || new_bms == 242 || new_bms == 243 || new_bms == 246 || new_bms == 254))
					 || (query_id == 60 && (new_bms == 3 || new_bms == 6 || new_bms == 7 || new_bms == 14 || new_bms == 15 || new_bms == 18 || new_bms == 19 || new_bms == 20 || new_bms == 22 || new_bms == 23 || new_bms == 28 || new_bms == 30 || new_bms == 31 || new_bms == 55 || new_bms == 66 || new_bms == 67 || new_bms == 70 || new_bms == 71 || new_bms == 78 || new_bms == 79 || new_bms == 82 || new_bms == 83 || new_bms == 86 || new_bms == 87 || new_bms == 94 || new_bms == 95 || new_bms == 114 || new_bms == 118 || new_bms == 119 || new_bms == 126 || new_bms == 130 || new_bms == 131 || new_bms == 134 || new_bms == 135 || new_bms == 140 || new_bms == 142 || new_bms == 143 || new_bms == 146 || new_bms == 147 || new_bms == 148 || new_bms == 150 || new_bms == 151 || new_bms == 156 || new_bms == 158 ||new_bms == 159 || new_bms == 194 || new_bms == 195 || new_bms == 198 || new_bms == 199 || new_bms == 206 || new_bms == 207 || new_bms == 210 || new_bms == 211 || new_bms == 214 || new_bms == 215 || new_bms == 222 || new_bms == 223 || new_bms == 242 || new_bms == 243 || new_bms == 246 || new_bms == 254))
					 || (query_id == 61 && (new_bms == 5 || new_bms == 13 || new_bms == 17 || new_bms == 19 || new_bms == 20 || new_bms == 21 || new_bms == 23 || new_bms == 28 || new_bms == 29 || new_bms == 31 || new_bms == 65 || new_bms == 69 || new_bms == 76 || new_bms == 77 || new_bms == 79 || new_bms == 81 || new_bms == 83 || new_bms == 84 || new_bms == 85 || new_bms == 87 || new_bms == 92 || new_bms == 93 || new_bms == 95))
					 || (query_id == 62 && (new_bms == 5 || new_bms == 13 || new_bms == 17 || new_bms == 19 || new_bms == 20 || new_bms == 21 || new_bms == 23 || new_bms == 28 || new_bms == 29 || new_bms == 31 || new_bms == 65 || new_bms == 69 || new_bms == 76 || new_bms == 77 || new_bms == 79 || new_bms == 81 || new_bms == 83 || new_bms == 84 || new_bms == 85 || new_bms == 87 || new_bms == 92 || new_bms == 93 || new_bms == 95))
					 || (query_id == 63 && (new_bms == 5 || new_bms == 13 || new_bms == 17 || new_bms == 19 || new_bms == 20 || new_bms == 21 || new_bms == 23 || new_bms == 28 || new_bms == 29 || new_bms == 31 || new_bms == 65 || new_bms == 69 || new_bms == 76 || new_bms == 77 || new_bms == 79 || new_bms == 81 || new_bms == 83 || new_bms == 84 || new_bms == 85 || new_bms == 87 || new_bms == 92 || new_bms == 93 || new_bms == 95))
					 || (query_id == 64 && (new_bms == 5 || new_bms == 13 || new_bms == 17 || new_bms == 19 || new_bms == 20 || new_bms == 21 || new_bms == 23 || new_bms == 28 || new_bms == 29 || new_bms == 31 || new_bms == 65 || new_bms == 69 || new_bms == 76 || new_bms == 77 || new_bms == 79 || new_bms == 81 || new_bms == 83 || new_bms == 84 || new_bms == 85 || new_bms == 87 || new_bms == 92 || new_bms == 93 || new_bms == 95))
					 || (query_id == 65 && (new_bms == 3 || new_bms == 5 || new_bms == 7 || new_bms == 13 || new_bms == 15 || new_bms == 17 || new_bms == 19 || new_bms == 20 || new_bms == 21 || new_bms == 23 || new_bms == 28 || new_bms == 29 || new_bms == 31 || new_bms == 51 || new_bms == 55 || new_bms == 65 || new_bms == 67 || new_bms == 69 || new_bms == 71 || new_bms == 76 || new_bms == 77 || new_bms == 79 || new_bms == 81 || new_bms == 83 || new_bms == 84 || new_bms == 85 || new_bms == 87 || new_bms == 92 || new_bms == 93 || new_bms == 95 || new_bms == 119))
					 || (query_id == 66 && (new_bms == 5 || new_bms == 13 || new_bms == 17 || new_bms == 19 || new_bms == 20 || new_bms == 21 || new_bms == 23 || new_bms == 28 || new_bms == 29 || new_bms == 31 || new_bms == 65 || new_bms == 69 || new_bms == 76 || new_bms == 77 || new_bms == 79 || new_bms == 81 || new_bms == 83 || new_bms == 84 || new_bms == 85 || new_bms == 87 || new_bms == 92 || new_bms == 93 || new_bms == 95))
					 || (query_id == 73 && (new_bms == 34 || new_bms == 35 || new_bms == 39 || new_bms == 40 || new_bms == 42 || new_bms == 43 || new_bms == 46 || new_bms == 47 || new_bms == 56 || new_bms == 58 || new_bms == 59 || new_bms == 62 || new_bms == 63 || new_bms == 104 || new_bms == 110 || new_bms == 111 || new_bms == 106 || new_bms == 107 || new_bms == 123 || new_bms == 126 || new_bms == 127 || new_bms == 170 || new_bms == 171 || new_bms == 174 || new_bms == 175 || new_bms == 187 || new_bms == 230 || new_bms == 231 || new_bms == 239 || new_bms == 298 || new_bms == 299 || new_bms == 302 || new_bms == 303 || new_bms == 315 || new_bms == 366 || new_bms == 426 || new_bms == 427 || new_bms == 431 || new_bms == 547 || new_bms == 551 || new_bms == 552 || new_bms == 554 || new_bms == 555 || new_bms == 558 || new_bms == 559 || new_bms == 570 || new_bms == 571 || new_bms == 575 || new_bms == 618 || new_bms == 619 || new_bms == 682 || new_bms == 683 || new_bms == 810 || new_bms == 811))
					 || (query_id == 74 && (new_bms == 41 || new_bms == 104 || new_bms == 109 || new_bms == 360 || new_bms == 365 || new_bms == 552 || new_bms == 553 || new_bms == 555 || new_bms == 557 || new_bms == 559 || new_bms == 616 || new_bms == 617 || new_bms == 619 || new_bms == 621 || new_bms == 623 || new_bms == 872 || new_bms == 873 || new_bms == 875 || new_bms == 877 || new_bms == 879))
					 || (query_id == 75 && (new_bms == 41))
					 || (query_id == 76 && (new_bms == 41 || new_bms == 552 || new_bms == 553 || new_bms == 555 || new_bms == 557 || new_bms == 559 || new_bms == 616 || new_bms == 617 || new_bms == 619 || new_bms == 621 || new_bms == 872  || new_bms == 873))
					 || (query_id == 87 && (new_bms == 143 || new_bms == 239))
					 || (query_id == 88 && (new_bms == 143 || new_bms == 239))
					 || (query_id == 92 && (new_bms == 2184 || new_bms == 2185 || new_bms == 2187 || new_bms == 2189 || new_bms == 2191 || new_bms == 2216 || new_bms == 2217 || new_bms == 2219 || new_bms == 2221 || new_bms == 2223 || new_bms == 2280 || new_bms == 2281 || new_bms == 2283 || new_bms == 2285 || new_bms == 2287 || new_bms == 2696 || new_bms == 2697 || new_bms == 2699 || new_bms == 2701 || new_bms == 2703 || new_bms == 2728 || new_bms == 2729 || new_bms == 2731 || new_bms == 2733 || new_bms == 2735 || new_bms == 2792 || new_bms == 2793 || new_bms == 2795 || new_bms == 2797 || new_bms == 2799 || new_bms == 3720 || new_bms == 3721 || new_bms == 3723 || new_bms == 3725 || new_bms == 3727 || new_bms == 3753 || new_bms == 3755 || new_bms == 3757 || new_bms == 3759 || new_bms == 3816 || new_bms == 3817 || new_bms == 3819 || new_bms == 3821 || new_bms == 3823))
					 || (query_id == 93 && (new_bms == 2184 || new_bms == 2185 || new_bms == 2187 || new_bms == 2189 || new_bms == 2191 || new_bms == 2216 || new_bms == 2217 || new_bms == 2219 || new_bms == 2221 || new_bms == 2223 || new_bms == 2280 || new_bms == 2281 || new_bms == 2283 || new_bms == 2285 || new_bms == 2287 || new_bms == 2696 || new_bms == 2697 || new_bms == 2699 || new_bms == 2701 || new_bms == 2703 || new_bms == 2728 || new_bms == 2729 || new_bms == 2731 || new_bms == 2733 || new_bms == 2735 || new_bms == 2792 || new_bms == 2793 || new_bms == 2795 || new_bms == 2797 || new_bms == 2799 || new_bms == 3720 || new_bms == 3721 || new_bms == 3723 || new_bms == 3725 || new_bms == 3727 || new_bms == 3753 || new_bms == 3755 || new_bms == 3757 || new_bms == 3759 || new_bms == 3816 || new_bms == 3817 || new_bms == 3819 || new_bms == 3821 || new_bms == 3823))
					 || (query_id == 94 && (new_bms == 168 || new_bms == 171 || new_bms == 173 || new_bms == 175 || new_bms == 169 || new_bms == 680 || new_bms == 681 || new_bms == 683 || new_bms == 685 || new_bms == 687 || new_bms == 1704 || new_bms == 1705 || new_bms == 1707 || new_bms == 1709 || new_bms == 1711 || new_bms == 2095 || new_bms == 2155 || new_bms == 2184 || new_bms == 2185 || new_bms == 2187 || new_bms == 2189 || new_bms == 2191 || new_bms == 2216 || new_bms == 2217 || new_bms == 2219 || new_bms == 2221 || new_bms == 2223 || new_bms == 2280 || new_bms == 2281 || new_bms == 2283 || new_bms == 2285 || new_bms == 2287 || new_bms == 2575 || new_bms == 2603 || new_bms == 2605 || new_bms == 2664 || new_bms == 2665 || new_bms == 2696 || new_bms == 2697 || new_bms == 2699 || new_bms == 2701 || new_bms == 2703 || new_bms == 2728 || new_bms == 2729 || new_bms == 2731 || new_bms == 2733 || new_bms == 2735 || new_bms == 2792 || new_bms == 2793 || new_bms == 2795 || new_bms == 2797 || new_bms == 2799 || new_bms == 3597 || new_bms == 3624 || new_bms == 3629 || new_bms == 3720 || new_bms == 3721 || new_bms == 3723 || new_bms == 3725 || new_bms == 3727 || new_bms == 3753 || new_bms == 3755 || new_bms == 3757 || new_bms == 3759 || new_bms == 3769 || new_bms == 3816 || new_bms == 3817 || new_bms == 3819 || new_bms == 3821 || new_bms == 3823))
					 || (query_id == 101 && (new_bms == 1091 || new_bms == 1219 || new_bms == 1347 || new_bms == 1859 || new_bms == 1867 || new_bms == 3403 || new_bms == 4419 || new_bms == 4427 || new_bms == 4443 || new_bms == 4459 || new_bms == 4931 || new_bms == 4939 || new_bms == 4955 || new_bms == 4971 || new_bms == 4987 || new_bms == 5059 || new_bms == 5123 || new_bms == 5131 || new_bms == 5147 || new_bms == 5186 || new_bms == 5187 || new_bms == 5194 || new_bms == 5195 || new_bms == 5210 || new_bms == 5211 || new_bms == 5226 || new_bms == 5227 || new_bms == 5242 || new_bms == 5243 || new_bms == 5314 || new_bms == 5315 || new_bms == 5322 || new_bms == 5323 || new_bms == 5338 || new_bms == 5339 || new_bms == 5354 || new_bms == 5355 || new_bms == 5370 || new_bms == 5371 || new_bms == 5379 || new_bms == 5387 || new_bms == 5403 || new_bms == 5419 || new_bms == 5435 || new_bms == 5442 || new_bms == 5443 || new_bms == 5450 || new_bms == 5451 || new_bms == 5466 || new_bms == 5467 || new_bms == 5482 || new_bms == 5483 || new_bms == 5498 || new_bms == 5499 || new_bms == 5570 || new_bms == 5571 || new_bms == 5578 || new_bms == 5579 || new_bms == 5594 || new_bms == 5595 || new_bms == 5610 || new_bms == 5611 || new_bms == 5626 || new_bms == 5627 || new_bms == 5891 || new_bms == 5899 || new_bms == 5915 || new_bms == 5931 || new_bms == 5947 || new_bms == 5954 || new_bms == 5955 || new_bms == 5962 || new_bms == 5963 || new_bms == 5978 || new_bms == 5979 || new_bms == 5994 || new_bms == 5995 || new_bms == 6010 || new_bms == 6011 || new_bms == 6082 || new_bms == 6083 || new_bms == 6090 || new_bms == 6091 || new_bms == 6106 || new_bms == 6107 || new_bms == 6122 || new_bms == 6123 || new_bms == 6138 || new_bms == 6139 || new_bms == 7171 || new_bms == 7234 || new_bms == 7235 || new_bms == 7242 || new_bms == 7243 || new_bms == 7258 || new_bms == 7259 || new_bms == 7274 || new_bms == 7275 || new_bms == 7290 || new_bms == 7291 || new_bms == 7362 || new_bms == 7363 || new_bms == 7370 || new_bms == 7371 || new_bms == 7387 || new_bms == 7403 || new_bms == 7419 || new_bms == 7427 || new_bms == 7435 || new_bms == 7490 || new_bms == 7491 || new_bms == 7498 || new_bms == 7499 || new_bms == 7514 || new_bms == 7515 || new_bms == 7530 || new_bms == 7531 || new_bms == 7546 || new_bms == 7547 || new_bms == 7618 || new_bms == 7619 || new_bms == 7626 || new_bms == 7627 || new_bms == 7642 || new_bms == 7643 || new_bms == 7659 || new_bms == 7675 || new_bms == 7939 || new_bms == 7947 || new_bms == 8002 || new_bms == 8003 || new_bms == 8010 || new_bms == 8011 || new_bms == 8026 || new_bms == 8027 || new_bms == 8042 || new_bms == 8043 || new_bms == 8058 || new_bms == 8059 || new_bms == 8130 || new_bms == 8131 || new_bms == 8138 || new_bms == 8139 || new_bms == 8154 || new_bms == 8155 || new_bms == 21442 || new_bms == 21827 || new_bms == 21835 || new_bms == 21851 || new_bms == 21883 || new_bms == 22339 || new_bms == 22347 || new_bms == 22363 || new_bms == 22379 || new_bms == 22395 || new_bms == 24411 || new_bms == 29546 || new_bms == 29547 || new_bms == 37954 || new_bms == 37955 || new_bms == 37962 || new_bms == 37963 || new_bms == 37978 || new_bms == 37979 || new_bms == 37994 || new_bms == 37995 || new_bms == 38011 || new_bms == 38083 || new_bms == 38091 || new_bms == 38107 || new_bms == 38123 || new_bms == 38139 || new_bms == 38210 || new_bms == 38211 || new_bms == 38218 || new_bms == 38219 || new_bms == 38234 || new_bms == 38235 || new_bms == 38250 || new_bms == 38251 || new_bms == 38266 || new_bms == 38267 || new_bms == 38338 || new_bms == 38339 || new_bms == 38346 || new_bms == 38347 || new_bms == 38362 || new_bms == 38363 || new_bms == 38379 || new_bms == 38395 || new_bms == 38659 || new_bms == 38722 || new_bms == 38723 || new_bms == 38730 || new_bms == 38731 || new_bms == 38746 || new_bms == 38747 || new_bms == 38762 || new_bms == 38763 || new_bms == 38778 || new_bms == 38779 || new_bms == 38850 || new_bms == 38851 || new_bms == 38858 || new_bms == 38859 || new_bms == 38874 || new_bms == 38875 || new_bms == 40003 || new_bms == 40011 || new_bms == 40027 || new_bms == 40043 || new_bms == 40131 || new_bms == 40258 || new_bms == 40259 || new_bms == 40266 || new_bms == 40267 || new_bms == 40283 || new_bms == 40299 || new_bms == 40315 || new_bms == 40387 || new_bms == 40395 || new_bms == 40411 || new_bms == 40770 || new_bms == 40771 || new_bms == 40778 || new_bms == 40779 || new_bms == 40794 || new_bms == 40795 || new_bms == 40899 || new_bms == 54347 || new_bms == 54595 || new_bms == 54603 || new_bms == 54619 || new_bms == 54651 || new_bms == 54723 || new_bms == 55107 || new_bms == 55115 || new_bms == 55131 || new_bms == 73067 || new_bms == 73083 || new_bms == 73547 || new_bms == 73563 || new_bms == 87899 || new_bms == 95563 || new_bms == 105819))
					 || (query_id == 102 && (new_bms == 1091 || new_bms == 1219 || new_bms == 1347 || new_bms == 1859 || new_bms == 1867 || new_bms == 3403 || new_bms == 4419 || new_bms == 4427 || new_bms == 4443 || new_bms == 4459 || new_bms == 4931 || new_bms == 4939 || new_bms == 4955 || new_bms == 4971 || new_bms == 4987 || new_bms == 5059 || new_bms == 5123 || new_bms == 5131 || new_bms == 5147 || new_bms == 5186 || new_bms == 5187 || new_bms == 5194 || new_bms == 5195 || new_bms == 5210 || new_bms == 5211 || new_bms == 5226 || new_bms == 5227 || new_bms == 5242 || new_bms == 5243 || new_bms == 5314 || new_bms == 5315 || new_bms == 5322 || new_bms == 5323 || new_bms == 5338 || new_bms == 5339 || new_bms == 5354 || new_bms == 5355 || new_bms == 5370 || new_bms == 5371 || new_bms == 5379 || new_bms == 5387 || new_bms == 5403 || new_bms == 5419 || new_bms == 5435 || new_bms == 5442 || new_bms == 5443 || new_bms == 5450 || new_bms == 5451 || new_bms == 5466 || new_bms == 5467 || new_bms == 5482 || new_bms == 5483 || new_bms == 5498 || new_bms == 5499 || new_bms == 5570 || new_bms == 5571 || new_bms == 5578 || new_bms == 5579 || new_bms == 5594 || new_bms == 5595 || new_bms == 5610 || new_bms == 5611 || new_bms == 5626 || new_bms == 5627 || new_bms == 5891 || new_bms == 5899 || new_bms == 5915 || new_bms == 5931 || new_bms == 5947 || new_bms == 5954 || new_bms == 5955 || new_bms == 5962 || new_bms == 5963 || new_bms == 5978 || new_bms == 5979 || new_bms == 5994 || new_bms == 5995 || new_bms == 6010 || new_bms == 6011 || new_bms == 6082 || new_bms == 6083 || new_bms == 6090 || new_bms == 6091 || new_bms == 6106 || new_bms == 6107 || new_bms == 6122 || new_bms == 6123 || new_bms == 6138 || new_bms == 6139 || new_bms == 7171 || new_bms == 7234 || new_bms == 7235 || new_bms == 7242 || new_bms == 7243 || new_bms == 7258 || new_bms == 7259 || new_bms == 7274 || new_bms == 7275 || new_bms == 7290 || new_bms == 7291 || new_bms == 7362 || new_bms == 7363 || new_bms == 7370 || new_bms == 7371 || new_bms == 7387 || new_bms == 7403 || new_bms == 7419 || new_bms == 7427 || new_bms == 7435 || new_bms == 7490 || new_bms == 7491 || new_bms == 7498 || new_bms == 7499 || new_bms == 7514 || new_bms == 7515 || new_bms == 7530 || new_bms == 7531 || new_bms == 7546 || new_bms == 7547 || new_bms == 7618 || new_bms == 7619 || new_bms == 7626 || new_bms == 7627 || new_bms == 7642 || new_bms == 7643 || new_bms == 7659 || new_bms == 7675 || new_bms == 7939 || new_bms == 7947 || new_bms == 8002 || new_bms == 8003 || new_bms == 8010 || new_bms == 8011 || new_bms == 8026 || new_bms == 8027 || new_bms == 8042 || new_bms == 8043 || new_bms == 8058 || new_bms == 8059 || new_bms == 8130 || new_bms == 8131 || new_bms == 8138 || new_bms == 8139 || new_bms == 8154 || new_bms == 8155 || new_bms == 21442 || new_bms == 21827 || new_bms == 21835 || new_bms == 21851 || new_bms == 21883 || new_bms == 22339 || new_bms == 22347 || new_bms == 22363 || new_bms == 22379 || new_bms == 22395 || new_bms == 24411 || new_bms == 29546 || new_bms == 29547 || new_bms == 37954 || new_bms == 37955 || new_bms == 37962 || new_bms == 37963 || new_bms == 37978 || new_bms == 37979 || new_bms == 37994 || new_bms == 37995 || new_bms == 38011 || new_bms == 38083 || new_bms == 38091 || new_bms == 38107 || new_bms == 38123 || new_bms == 38139 || new_bms == 38210 || new_bms == 38211 || new_bms == 38218 || new_bms == 38219 || new_bms == 38234 || new_bms == 38235 || new_bms == 38250 || new_bms == 38251 || new_bms == 38266 || new_bms == 38267 || new_bms == 38338 || new_bms == 38339 || new_bms == 38346 || new_bms == 38347 || new_bms == 38362 || new_bms == 38363 || new_bms == 38379 || new_bms == 38395 || new_bms == 38659 || new_bms == 38722 || new_bms == 38723 || new_bms == 38730 || new_bms == 38731 || new_bms == 38746 || new_bms == 38747 || new_bms == 38762 || new_bms == 38763 || new_bms == 38778 || new_bms == 38779 || new_bms == 38850 || new_bms == 38851 || new_bms == 38858 || new_bms == 38859 || new_bms == 38874 || new_bms == 38875 || new_bms == 40003 || new_bms == 40011 || new_bms == 40027 || new_bms == 40043 || new_bms == 40131 || new_bms == 40258 || new_bms == 40259 || new_bms == 40266 || new_bms == 40267 || new_bms == 40283 || new_bms == 40299 || new_bms == 40315 || new_bms == 40387 || new_bms == 40395 || new_bms == 40411 || new_bms == 40770 || new_bms == 40771 || new_bms == 40778 || new_bms == 40779 || new_bms == 40794 || new_bms == 40795 || new_bms == 40899 || new_bms == 54347 || new_bms == 54595 || new_bms == 54603 || new_bms == 54619 || new_bms == 54651 || new_bms == 54723 || new_bms == 55107 || new_bms == 55115 || new_bms == 55131 || new_bms == 73067 || new_bms == 73083 || new_bms == 73547 || new_bms == 73563 || new_bms == 87899 || new_bms == 95563 || new_bms == 105819))
					 || (query_id == 103 && (new_bms == 1091 || new_bms == 1219 || new_bms == 1347 || new_bms == 1859 || new_bms == 1867 || new_bms == 3403 || new_bms == 4419 || new_bms == 4427 || new_bms == 4443 || new_bms == 4459 || new_bms == 4931 || new_bms == 4939 || new_bms == 4955 || new_bms == 4971 || new_bms == 4987 || new_bms == 5059 || new_bms == 5123 || new_bms == 5131 || new_bms == 5147 || new_bms == 5186 || new_bms == 5187 || new_bms == 5194 || new_bms == 5195 || new_bms == 5210 || new_bms == 5211 || new_bms == 5226 || new_bms == 5227 || new_bms == 5242 || new_bms == 5243 || new_bms == 5314 || new_bms == 5315 || new_bms == 5322 || new_bms == 5323 || new_bms == 5338 || new_bms == 5339 || new_bms == 5354 || new_bms == 5355 || new_bms == 5370 || new_bms == 5371 || new_bms == 5379 || new_bms == 5387 || new_bms == 5403 || new_bms == 5419 || new_bms == 5435 || new_bms == 5442 || new_bms == 5443 || new_bms == 5450 || new_bms == 5451 || new_bms == 5466 || new_bms == 5467 || new_bms == 5482 || new_bms == 5483 || new_bms == 5498 || new_bms == 5499 || new_bms == 5570 || new_bms == 5571 || new_bms == 5578 || new_bms == 5579 || new_bms == 5594 || new_bms == 5595 || new_bms == 5610 || new_bms == 5611 || new_bms == 5626 || new_bms == 5627 || new_bms == 5891 || new_bms == 5899 || new_bms == 5915 || new_bms == 5931 || new_bms == 5947 || new_bms == 5954 || new_bms == 5955 || new_bms == 5962 || new_bms == 5963 || new_bms == 5978 || new_bms == 5979 || new_bms == 5994 || new_bms == 5995 || new_bms == 6010 || new_bms == 6011 || new_bms == 6082 || new_bms == 6083 || new_bms == 6090 || new_bms == 6091 || new_bms == 6106 || new_bms == 6107 || new_bms == 6122 || new_bms == 6123 || new_bms == 6138 || new_bms == 6139 || new_bms == 7171 || new_bms == 7234 || new_bms == 7235 || new_bms == 7242 || new_bms == 7243 || new_bms == 7258 || new_bms == 7259 || new_bms == 7274 || new_bms == 7275 || new_bms == 7290 || new_bms == 7291 || new_bms == 7362 || new_bms == 7363 || new_bms == 7370 || new_bms == 7371 || new_bms == 7387 || new_bms == 7403 || new_bms == 7419 || new_bms == 7427 || new_bms == 7435 || new_bms == 7490 || new_bms == 7491 || new_bms == 7498 || new_bms == 7499 || new_bms == 7514 || new_bms == 7515 || new_bms == 7530 || new_bms == 7531 || new_bms == 7546 || new_bms == 7547 || new_bms == 7618 || new_bms == 7619 || new_bms == 7626 || new_bms == 7627 || new_bms == 7642 || new_bms == 7643 || new_bms == 7659 || new_bms == 7675 || new_bms == 7939 || new_bms == 7947 || new_bms == 8002 || new_bms == 8003 || new_bms == 8010 || new_bms == 8011 || new_bms == 8026 || new_bms == 8027 || new_bms == 8042 || new_bms == 8043 || new_bms == 8058 || new_bms == 8059 || new_bms == 8130 || new_bms == 8131 || new_bms == 8138 || new_bms == 8139 || new_bms == 8154 || new_bms == 8155 || new_bms == 21442 || new_bms == 21827 || new_bms == 21835 || new_bms == 21851 || new_bms == 21883 || new_bms == 22339 || new_bms == 22347 || new_bms == 22363 || new_bms == 22379 || new_bms == 22395 || new_bms == 24411 || new_bms == 29546 || new_bms == 29547 || new_bms == 37954 || new_bms == 37955 || new_bms == 37962 || new_bms == 37963 || new_bms == 37978 || new_bms == 37979 || new_bms == 37994 || new_bms == 37995 || new_bms == 38011 || new_bms == 38083 || new_bms == 38091 || new_bms == 38107 || new_bms == 38123 || new_bms == 38139 || new_bms == 38210 || new_bms == 38211 || new_bms == 38218 || new_bms == 38219 || new_bms == 38234 || new_bms == 38235 || new_bms == 38250 || new_bms == 38251 || new_bms == 38266 || new_bms == 38267 || new_bms == 38338 || new_bms == 38339 || new_bms == 38346 || new_bms == 38347 || new_bms == 38362 || new_bms == 38363 || new_bms == 38379 || new_bms == 38395 || new_bms == 38659 || new_bms == 38722 || new_bms == 38723 || new_bms == 38730 || new_bms == 38731 || new_bms == 38746 || new_bms == 38747 || new_bms == 38762 || new_bms == 38763 || new_bms == 38778 || new_bms == 38779 || new_bms == 38850 || new_bms == 38851 || new_bms == 38858 || new_bms == 38859 || new_bms == 38874 || new_bms == 38875 || new_bms == 40003 || new_bms == 40011 || new_bms == 40027 || new_bms == 40043 || new_bms == 40131 || new_bms == 40258 || new_bms == 40259 || new_bms == 40266 || new_bms == 40267 || new_bms == 40283 || new_bms == 40299 || new_bms == 40315 || new_bms == 40387 || new_bms == 40395 || new_bms == 40411 || new_bms == 40770 || new_bms == 40771 || new_bms == 40778 || new_bms == 40779 || new_bms == 40794 || new_bms == 40795 || new_bms == 40899 || new_bms == 54347 || new_bms == 54595 || new_bms == 54603 || new_bms == 54619 || new_bms == 54651 || new_bms == 54723 || new_bms == 55107 || new_bms == 55115 || new_bms == 55131 || new_bms == 73067 || new_bms == 73083 || new_bms == 73547 || new_bms == 73563 || new_bms == 87899 || new_bms == 95563 || new_bms == 105819))
					 || (query_id == 104 && (new_bms == 1595))
					 || (query_id == 105 && (new_bms == 1001 || new_bms == 1595 || new_bms == 4027))
					 || (query_id == 106 && (new_bms == 97 || new_bms == 1001 || new_bms == 1595 || new_bms == 4027 || new_bms == 4072))
					 || (query_id == 107 && (new_bms == 276 || new_bms == 1349 || new_bms == 1873))
					 || (query_id == 108 && (new_bms == 276 || new_bms == 1349 || new_bms == 1873))
					 || (query_id == 109 && (new_bms == 276 || new_bms == 341 || new_bms == 343 || new_bms == 371 || new_bms == 373 || new_bms == 375 || new_bms == 383 || new_bms == 455 || new_bms == 471 || new_bms == 499 || new_bms == 503 || new_bms == 887 || new_bms == 1143 || new_bms == 1239 || new_bms == 1271 || new_bms == 1335 || new_bms == 1349 || new_bms == 1351 || new_bms == 1364 || new_bms == 1365 || new_bms == 1367 || new_bms == 1395 || new_bms == 1397 || new_bms == 1399 || new_bms == 1396 || new_bms == 1407 || new_bms == 1475 || new_bms == 1477 || new_bms == 1479 || new_bms == 1489 || new_bms == 1491 || new_bms == 1492 || new_bms == 1493 || new_bms == 1495 || new_bms == 1523 || new_bms == 1525 || new_bms == 1527 || new_bms == 1873 || new_bms == 1911))) {
						continue;
					}
					bool found = false;
					char* pos;
					char* p;
					char cmp[1000];
					int relid_in_file;
					FILE* f_check = fopen(query_sql, "r+");
					while(fgets(cmp, 1000, f_check) != NULL) {
						if((pos = strchr(cmp, '\n')) != NULL) {
							*pos = '\0';
						}
						p = strtok(cmp, ":");
						relid_in_file = atoi(p);
						if(relid_in_file == (new_bms << 1)) {
							found = true;
							break;
						}
					}
					fclose(f_check);
					if(found) {
						continue;
					}
					FILE* f_write = fopen(query_sql, "a+");
					fprintf(f_write, "%d:", (new_bms << 1));
					fclose(f_write);
					std::cout << subquery;
					DBConfig* db_config = new DBConfig();
					db_config->SetOptionByName("threads", 15);
					DuckDB db("../giveDuckTopDownOptimizer/JoinOrderBenchmark.db", db_config);
					Connection con(db);
					auto result = con.Query(subquery.c_str());
					rel r = { bms << 1, result->RowCount() };
					f_write = fopen(query_sql, "a+");
					fprintf(f_write, "%lf\n", r.rows);
					fclose(f_write);
					delete db_config;
				}
			}
		}
	}
	fclose(f_query);
}
