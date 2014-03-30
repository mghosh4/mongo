class HungarianAlgo{
public: 
   void max_cost_assignment(long long ** datainkr, int numRow, int numColumn, int assignment[]);
   ~HungarianAlgo(){
    delete[] cost;
    delete[] mask;
    delete[] path;
   }
   void min_cost_assignment(long long ** datainkr, int numRow, int numColumn, int assignment[]);
private:
   void main_algorithm(int assisgnment[]);
   void step_one(int* step);
   void step_two(int* step);
   void step_three(int* step);
   void step_four(int* step);
   void step_five(int* step);
   void step_six(int* step);
   void step_seven(int* step);
   void find_smallest(long long* minval);
   void erase_primes();
   void clear_covers();
   void augment_path();
   void find_prime_in_row(int r, int* c);
   void find_star_in_col(int c, int* r);
   void find_star_in_row(int row, int* col);
   bool star_in_row(int row);
   void find_a_zero(int* row, int* col);

long long **cost;
bool *ColCover;
bool *RowCover;
int ncol;
int nrow;
int **path;
int **mask;
int path_row_0;
int path_col_0;
int path_count;
};
