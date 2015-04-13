//Illinois Open Source License
//
//University of Illinois
//Open Source License
//
//Copyright © 2015,    Board of Trustees of the University of Illinois.  All rights reserved.
//
//Developed by:
//
// Distributed Protocols Research Group in the Department of Computer Science
// The University of Illinois at Urbana-Champaign
// http://dprg.cs.uiuc.edu/

//Mainak Ghosh, mghosh4@illinois.edu
//Wenting Wang, wwang84@illinois.edu
//Gopalakrishna Holla, hollava2@illinois.edu
//Indranil Gupta, indy@cs.uiuc.edu
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal with the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
//    * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimers.
//    * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimers in the documentation and/or other materials provided with the distribution.
//    * Neither the names of The Monet Group or The University of Illinois at Urbana-Champaign, nor the names of its contributors may be used to endorse or promote products derived from this Software without specific prior written permission.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
//PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
//AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE SOFTWARE.
#include "HungarianAlgo.h"

#include <iostream>
#include <cmath>
#include <climits>
#include <string.h>
void HungarianAlgo::max_cost_assignment(long long ** datainkr, int numRow, int numColumn,int assignment[])
{
    long long max=0;     
    for (  int row = 0 ; row < numRow ; row++ ) {
        for (  int col = 0 ; col < numColumn ; col++ ) {
            if(max<datainkr[row][col])
                max = datainkr[row][col];
        }
    }
    
    for (  int row = 0 ; row < numRow ; row++ ) {
        for (  int col = 0 ; col < numColumn ; col++ ) {
            datainkr[row][col]=max-datainkr[row][col];
            std::cout.width(8);
            std::cout << datainkr[row][col] << ",";
              
        }
        std::cout << std::endl;
    }
    std::cout << std::endl;
	
    min_cost_assignment(datainkr,numRow,numColumn,assignment);
}
void HungarianAlgo::min_cost_assignment(long long **datainkr, int numRow, int numColumn, int assignment[])
{
   
    //initialize path and mask; and copy cost
    bool flip = false;
    if (numRow < numColumn )
    {
        flip = true;
        nrow = numColumn;
        ncol = numColumn;
    } else {
        nrow = numRow;
        ncol = numRow;
    }
    int* result= new int[ncol];
    cost = new long long*[nrow];
    for (int i = 0; i < nrow; i++)
    {
        cost[i] = new long long[ncol];
    }

    for (int i = 0; i < numRow; i++)
    {
        for (int j = 0; j < numColumn; j++)
        {
                if(flip)
                     cost[j][i] = datainkr[i][j];
                else
                     cost[i][j] = datainkr[i][j];
        }
    }
    if (numRow != numColumn ){
	int smaller=numRow>numColumn?numColumn:numRow;
       //find the largest number in the matrix
       int max_cost = datainkr[0][0];
       for (int r = 0;r<numRow;r++)
          for (int c = 0; c< numColumn; c++)
               if(max_cost<datainkr[r][c])
                   max_cost = datainkr[r][c];
       for (int r = 0;r<nrow;r++){
           for (int c = smaller; c< nrow;c++){
                   cost[r][c]=max_cost;
           }
       }
    }
	#ifdef DEBUG
	std::cout << "Cost input matrix:" << std::endl;
	  for (  int row = 0 ; row < nrow ; row++ ) {
	    for (  int col = 0 ; col < ncol ; col++ ) {
	      std::cout.width(8);
	      std::cout << cost[row][col] << ",";
              
            }
	    std::cout << std::endl;
          }
	  std::cout << std::endl;
	#endif
   
    mask = new int *[nrow];
    path = new int *[2 * nrow +1];
    
    for (int i = 0; i < (2 * nrow +1); i++)
    {
        path[i]=new int[2];
    }
    for (int i = 0; i <(2 * nrow +1); i++)
    {
        for (int j = 0; j < 2; j++)
        {
            path[i][j] = 0;
        }
    }

    for (int i = 0; i < nrow; i++)
    {
        mask[i] = new int[ncol];

    }
    
    for (int i = 0; i < nrow; i++)
    {
        for (int j = 0; j < ncol; j++)
        {
            mask[i][j] = 0;
        }
    }
    
    //initial cover
    ColCover = new bool[ncol];
    RowCover = new bool[nrow];
   
    for (  int i = 0 ; i < ncol ; i++ ) {
        ColCover[i] = false;
    }

    for (  int i = 0 ; i < nrow ; i++ ) {
        RowCover[i] = false;
    }

    main_algorithm(result);
    if(flip){
        for(int c = 0;c<numColumn;c++){
            assignment[result[c]]=c;
        }
    } else {
        for(int c = 0;c<numRow;c++){
	     assignment[c]=result[c];
      }
    }
   delete result;
}
void HungarianAlgo::main_algorithm(int result[])
{
    if (ncol != nrow)
    {
       std::cout<<"not square matrix"<<std::endl;
       return;
    }
    bool done = false;
    int step = 1;
    while(!done)
    {
        #ifdef DEBUG
    std::cout << "step:" << step <<std::endl;
        #endif

        switch(step)
        {
            case 1:
                    step_one(&step);
                    break;
            case 2:
                    step_two(&step);
                    break;
            case 3:
                    step_three(&step);
                    break;
            case 4:
                    step_four(&step);
                    break;
            case 5:
                    step_five(&step);
                    break;
            case 6:
                    step_six(&step);
                    break;
            case 7:
                    step_seven(&step);
                    done=true;
                    break;
        }
        //showCostMatrix();
    #ifdef DEBUG
    std::cout << "Cost matrix:" << std::endl;
      for (  int row = 0 ; row < nrow ; row++ ) {
        for (  int col = 0 ; col < ncol ; col++ ) {
          std::cout.width(8);
              if (mask[row][col] == 1 )
              std::cout << this->cost[row][col] <<"*"<< ",";
              else if (mask[row][col] == 2 )
              std::cout << this->cost[row][col] <<"'"<< ",";
              else
              std::cout << this->cost[row][col] << ",";
        }
        std::cout << std::endl;
      }
      std::cout << std::endl;
       
    std::cout << "row cover:" << std::endl;
      for (  int row = 0 ; row < nrow ; row++ ) {
          std::cout.width(8);
          std::cout << this->RowCover[row] << ",";
      }
      std::cout << std::endl;
    std::cout << "col cover:" << std::endl;
      for (  int col = 0 ; col < ncol ; col++ ) {
          std::cout.width(8);
          std::cout << this->ColCover[col] << ",";
      }
      std::cout << std::endl;
    #endif
    }

    for (int i=0;i<nrow;i++) 
    {
         for (int j = 0; j < ncol; j++)
        {
            if( mask[i][j] ==1 )
            {
                result[i]=j;
            }
        }
    }
}


/*For each row of the cost matrix, find the smallest element and substract it from every element in its row. When finished, Go to Step 2
*/
void HungarianAlgo::step_one(int* step){
    long long min_in_row;
    for(int r = 0; r < nrow; r++)
    {
        min_in_row = cost[r][0];
        for(int c = 0; c < ncol; c++)
            if(cost[r][c] < min_in_row)
                min_in_row = cost[r][c];
        for(int c=0;c<ncol;c++)
            cost[r][c] -= min_in_row;
    }
    *step=2;
}
/*Find a zero(Z) in the resulting matrix. If there is no starred zero in its row or column, star Z. Repeat for each element in the matrix. Go to Step 3;
*/
void HungarianAlgo::step_two(int* step)
{
    for (int r = 0; r < nrow; r++)
    {
        for (int c = 0; c < ncol; c++)
        {
            if(cost[r][c] == 0 && !RowCover[r] && !ColCover[c])
            {
                mask[r][c]=1;
                RowCover[r]=true;
                ColCover[c]=true;
            }
        }
        
    }
    for (int r = 0; r < nrow; r++)
    {
        RowCover[r]=false;
    }

    for (int c = 0; c < ncol; c++)
    {
         ColCover[c]=false;
    }
    *step=3; 
}

/*Cover each column containing a starred Zero. If K column are covered, the starred zeros describe a complete set of unique assignments. In this case, Go to Done, otherwise, Go to Step 4.
*/
void HungarianAlgo::step_three(int* step){
    int colcount = 0;
    for (int r= 0; r<nrow;r++)
    {
        for(int c = 0; c<ncol;c++)
        {
            if(mask[r][c]==1)
            {
                ColCover[c] = true;
            }
        }
    }
    for (int c =0;c<ncol;c++)
    {
        if(ColCover[c])
        {
            colcount++;
        }
    }
    if (colcount>=ncol)
       *step = 7;
    else
       *step = 4;
}

void HungarianAlgo::step_four(int* step){
    int row = -1;
    int col = -1;
    bool done = false;
    
    while(!done)
    {
        find_a_zero(&row,&col);
        if (row == -1)
        {
            done = true;
            *step = 6;
        }
        else
        {
           mask[row][col] = 2;
           if (star_in_row(row))
           {
               find_star_in_row(row,&col);
               RowCover[row]=true;
               ColCover[col]=false;
           }
           else
           {
               done = true;
               *step = 5;
               path_row_0=row;
               path_col_0=col;
           }
        }
    }
    
}
void HungarianAlgo::find_a_zero(int* row, int* col)
{
    int r = 0;
    int c;
    *row = -1;
    *col = -1;
    bool done = false;
    while (!done)
    {
         c = 0;
         while (true) 
         {
             if(cost[r][c]==0 && !RowCover[r] && !ColCover[c])
             {
                  *row = r;
                  *col = c;
                  done = true;
             }
         c += 1;
        if(c>=ncol ||done )
                break;
         }
         r+=1;
         if(r>=ncol)
            done=true;
    }
}

bool HungarianAlgo::star_in_row(int row)
{
    bool tmp = false;
    for (int c = 0; c< ncol; c++)
    {
        if(mask[row][c]==1)
        {
            tmp=true;
        }
    }
    return tmp;
}

void HungarianAlgo::find_star_in_row(int row, int* col)
{
    *col=-1;
    for (int c = 0; c < ncol; c++)
    {
        if(mask[row][c]==1)
        {
            *col = c;
        }
    }
}
void HungarianAlgo::step_five(int* step)
{
    bool done;
    int r = -1;
    int c = -1;

    path_count = 1;
    path[path_count - 1][0] = path_row_0;
    path[path_count - 1][1] = path_col_0;
    done = false;
    
    while (!done)
    {
    find_star_in_col(path[path_count - 1][1], &r);
        if (r > -1)
        {
            path_count += 1;
            path[path_count - 1][0] = r;
            path[path_count - 1][1] = path[path_count - 2][1];
        }
        else
            done = true;
        if (!done)
        {
            find_prime_in_row(path[path_count - 1][0], &c);
            path_count += 1;
            path[path_count - 1][0] = path[path_count - 2][0];
            path[path_count - 1][1] = c;
        }
    }
    augment_path();
    clear_covers();
    erase_primes();
    *step=3;
    //std::cout<<*step<<std::endl;
}

void HungarianAlgo::find_star_in_col(int c, int* r)
{
    *r = -1;
    for (int i = 0; i < nrow; i++)
        if (mask[i][c] == 1)
            *r = i;
}

void HungarianAlgo::find_prime_in_row(int r, int* c)
{
    for (int j = 0; j < ncol; j++)
        if (mask[r][j] == 2)
            *c = j;
}

void HungarianAlgo::augment_path()
{
    for (int p = 0; p < path_count; p++)
        if (mask[path[p][0]][path[p][1]] == 1)
            mask[path[p][0]][path[p][1]] = 0;
        else
            mask[path[p][0]][path[p][1]] = 1;
}

void HungarianAlgo::clear_covers()
{
    for (int r = 0; r < nrow; r++)
        RowCover[r] = false;
    for (int c = 0; c < ncol; c++)
        ColCover[c] = false;
}

void HungarianAlgo::erase_primes()
{
    for (int r = 0; r < nrow; r++)
        for (int c = 0; c < ncol; c++)
            if (mask[r][c] == 2)
                mask[r][c] = 0;
}


void HungarianAlgo::step_six(int* step)
{
    long long minval = LLONG_MAX;
    find_smallest(&minval);
    for (int r = 0; r < nrow; r++)
        for (int c = 0; c < ncol; c++)
        {
            if (RowCover[r] == 1)
                cost[r][c] += minval;
            if (ColCover[c] == 0)
                cost[r][c] -= minval;
        }
    *step = 4;
    //std::cout<<*step<<std::endl;
}
void HungarianAlgo::find_smallest(long long* minval)
{
    for (int r = 0; r < nrow; r++)
        for (int c = 0; c < ncol; c++)
            if ( ! RowCover[r] && !ColCover[c])
                if ( *minval > cost[r][c])
                    *minval = cost[r][c];
}

void HungarianAlgo::step_seven(int* step)
{
    #ifdef DEBUG
    std::cout<<"\n\n---------Run Complete----------"<<std::endl;
    #endif
}


