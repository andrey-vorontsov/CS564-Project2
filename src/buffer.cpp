/**
 * CS564 - Project 2 Group 62 submission
 * Andrey Vorontsov 
 * avorontsov
 * Benjamin Procknow
 * bprocknow
 * Phil Akaishi
 * pakaishi
 */

/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
  for (FrameId i = 0; i < numBufs; i++)
  {
    if (bufDescTable[i].dirty == true)
    {
      bufDescTable[i].file->writePage(bufPool[i]);
    }
  }
  delete hashTable;
  delete [] bufDescTable;
  delete [] bufPool;
}

void BufMgr::advanceClock()
{
  clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{ 
  uint32_t cntLoops = 0;
  while (cntLoops < (2 * numBufs))
  {
    advanceClock();
    BufDesc &curr = bufDescTable[clockHand];
    if (!curr.valid)
    {
      break;
    }
    if (!curr.refbit && curr.pinCnt == 0)
    {
      if (curr.dirty)
      {
        // flush dirty page to disk
        curr.file->writePage(bufPool[clockHand]);
      }
      hashTable->remove(curr.file, curr.pageNo);
      break;
    }
    bufDescTable[clockHand].refbit = false;
    cntLoops++;
  }
  if (cntLoops >= (2 * numBufs))
  {
    // throw BufferExceededException if all buffer frames are pinned
    throw BufferExceededException();
  }
  // return value
  frame = clockHand;
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  FrameId frameNo;
  try
  {
    hashTable->lookup(file, pageNo, frameNo);
    // success
    bufDescTable[frameNo].refbit = true;
    bufDescTable[frameNo].pinCnt++;
    // return value
    page = &bufPool[frameNo];
  }
  catch (const HashNotFoundException&)
  {
    // failure, allocate new page in buffer
    allocBuf(frameNo);
    bufPool[frameNo] = file->readPage(pageNo);
    hashTable->insert(file, pageNo, frameNo);
    bufDescTable[frameNo].Set(file, pageNo);
    // return value
    page = &bufPool[frameNo];
  }
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  //ben
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
// ben
}

void BufMgr::flushFile(const File* file) 
// ROUGH DRAFT
{ // can you access bufTable from file this way?
	for (i = 0; i < numBufs ; i++)
  {
    if(bufDescTable[i].file == file) // might need another way to access bufTable from file
    {
      if(bufTable[i].pinCnt > 0)
      {
        throw PagePinnedExcpetion();
      }
      if (bufTable[i].valid == false)
      {
        throw BadBufferException();
      }
      
      if(bufTable[i].dirty == true)
      {
        file->writePage();
        bufTable[i].dirty = false;
      }
      
      hashTable[i].remove(file, );
      bufTable[i].frameNo = BufDesc.Clear()
    }
  }  
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
// ROUGH DRAFT
  for (i = 0; i < bufPool.size; i++)
  {
    if (bufPool[i] == PageID)
    {
    free(bufPool[i]); // frees the page entry in bufPool
    }
  }
  for (j = 0; j < hashTable.size; j++)
  {
    if (hashTable[j] == PageID)
    {
    free(hashTable[j]); // frees the page entry in the hashTable
    }
  }
  file->delete(PageNo); // delete the allocated page from using allocPage
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
