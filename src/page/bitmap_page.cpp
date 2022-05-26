#include "page/bitmap_page.h"

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_>=GetMaxSupportedSize())
     {
     //  printf("Cannot allocate pages.Page_allocated is %u\n",page_allocated_);
       return false;  
   }
 //  printf("Next freepage is %u,Page allocate is %u,Allocate success!\n",next_free_page_,page_allocated_);
  page_allocated_++;
  //byte[next_free_page/8] |=play
  uint8_t play=0x80;
   for(uint8_t i=0;i< next_free_page_%8;i++)
   {
       play=play>>1;
   }
   bytes[next_free_page_/8]=(bytes[next_free_page_/8])|(play); //current locate turns into 1.
   page_offset=next_free_page_;
  //update the next free page.
   if(page_allocated_==GetMaxSupportedSize())//Arrive the max amount
      {

        return true;
      }
    for(size_t i=page_offset+1; i!=page_offset; i=(i+1)%GetMaxSupportedSize())
    {
       if(IsPageFreeLow(i/8,i%8)) //Page must be found and is free. 
       {
              
               next_free_page_=i;
               break;
       }
    }
    return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  //full
   if(page_offset>=GetMaxSupportedSize())
   {
     return false;
   }
   //empty
   if(page_allocated_==0)
   {
       return false;
   } 
 //  byte[page_offset/8]'s pageoffset%8 bit turn into 0
   uint8_t play=0x80;
   for(uint8_t i=0;i< page_offset%8;i++)//the page_offset%8 -th bit
   {
       play=play>>1;
   }
   if((bytes[page_offset/8]&play)==0) 
   {
     return false;
   }
   //Deallocate the page.
   play=(~play);
   bytes[page_offset/8]&=play; //Current locate turns into 0;
   page_allocated_--;
   next_free_page_=page_offset;
   //printf("Page %u is deallocated,next_free_page=%u,page_allocated=%u\n",page_offset,next_free_page_,page_allocated_);
   return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_offset>=GetMaxSupportedSize()){
    return false;
  }  
  return    IsPageFreeLow(page_offset/8,page_offset%8);
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  //byte[byte_index] & (128>>1 bit_index times)
  uint8_t play=0x80;
   for(uint8_t i=0;i<bit_index;i++)
   {
       play=play>>1;
   }
    if((bytes[byte_index] &play)!=0)//1:Not free.
    {    
        return false;
    }
    else{  //0:free.
      return true;
    }
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;