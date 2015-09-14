//Illinois Open Source License
//
//University of Illinois
//Open Source License
//
//Copyright © 2014,    Board of Trustees of the University of Illinois.  All rights reserved.
//
//Developed by:
//
// Distributed Protocols Research Group in the Department of Computer Science
// The University of Illinois at Urbana-Champaign
// http://dprg.cs.uiuc.edu/
// This is for the Project Morphus. The paper can be found at the website http://dprg.cs.uiuc.edu
//Mainak Ghosh, mghosh4@illinois.edu
//Wenting Wang, wwang84@illinois.edu
//Gopalakrishna Holla, vgkholla@gmail.com
//Indranil Gupta, indy@cs.uiuc.edu
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal with the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
//    * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimers.
//    * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimers in the documentation and/or other materials provided with the distribution.
//    * Neither the names of The Distributed Protocols Research Group (DPRG) or The University of Illinois at Urbana-Champaign, nor the names of its contributors may be used to endorse or promote products derived from this Software without specific prior written permission.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
//PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
//AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE SOFTWARE.


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

