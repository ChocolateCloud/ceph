// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
#include "erasure-code/cc/ErasureCodeCC.h"
#include "erasure-code/ErasureCode.h"
#include "gtest/gtest.h"
#include <string>
#include <stdlib.h>
#include <ctime>

using namespace ceph;

const unsigned SIMD_ALIGN = 32;

void print_memory(const void* ptr, int n)
{
  for(int i=0; i < n; i++)
    std::cout<<std::hex<<setfill('0')<<setw(2)<<(255&(short)*((char*)ptr+i));
  std::cout<<std::endl;
}

TEST(ErasureCodeCC, init_RS_default) //k=2, m=1
{
  std::string directory("directory");
  ErasureCodeCCRS cc(directory);
  EXPECT_EQ(2u, cc.get_data_chunk_count());
  EXPECT_EQ(1u, cc.get_coding_chunk_count());
  EXPECT_EQ(3u, cc.get_chunk_count());
  EXPECT_EQ(10u, cc.get_chunk_size(20));
}

TEST(ErasureCodeCC, init_RS)
{
  std::string directory("directory");
  ErasureCodeProfile profile;
  profile["plugin"] = "cc";
  profile["ruleset-failure-domain"] = "osd";
  profile["k"] = "4";
  profile["m"] = "3";
  
  ErasureCodeCCRS cc(directory);
  int err = cc.init(profile, &cerr);
  EXPECT_EQ(4u, cc.get_data_chunk_count());
  EXPECT_EQ(3u, cc.get_coding_chunk_count());
  EXPECT_EQ(7u, cc.get_chunk_count());
  EXPECT_EQ(5u, cc.get_chunk_size(20));
  EXPECT_EQ(0, err);
}

TEST(ErasureCodeCC, init_RLNC)
{
  std::string directory("directory");
  ErasureCodeProfile profile;
  profile["plugin"] = "cc";
  profile["ruleset-failure-domain"] = "osd";
  profile["k"] = "4";
  profile["m"] = "3";
  
  ErasureCodeCCRLNC cc(directory);
  int err = cc.init(profile, &cerr);
  EXPECT_EQ(4u, cc.get_data_chunk_count());
  EXPECT_EQ(3u, cc.get_coding_chunk_count());
  EXPECT_EQ(7u, cc.get_chunk_count());
  EXPECT_EQ(5u, cc.get_chunk_size(20));
  EXPECT_EQ(0, err);
}

TEST(ErasureCodeCC, encode_RS)
{
  unsigned int _chunk_size = 25;
  unsigned int _data_chunk_count = 4;
  unsigned int _coded_chunk_count = 3;
  unsigned int _chunk_count = _data_chunk_count + _coded_chunk_count;

  //init
  std::string directory("directory");
  ErasureCodeProfile profile;
  profile["plugin"] = "cc";
  profile["ruleset-failure-domain"] = "osd";
  profile["k"] = std::to_string(_data_chunk_count);
  profile["m"] = std::to_string(_coded_chunk_count);;
  
  ErasureCodeCCRS cc(directory);
  int err = cc.init(profile, &cerr);
  EXPECT_EQ(0, err);
  
  //encode
  set<int> want_to_encode;
  map<int, bufferlist> encoded;
  bufferlist in;
  bufferptr buf(buffer::create_aligned(_chunk_size * _data_chunk_count, SIMD_ALIGN));
  std::generate_n(&buf[0], _chunk_size * _data_chunk_count, rand);
  in.push_back(std::move(buf));
  
  for (unsigned int i = 0; i < cc.get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  err = cc.encode(want_to_encode, in, &encoded);
  
  EXPECT_EQ(_chunk_count, cc.get_chunk_count());  
  EXPECT_EQ(0, err);
  EXPECT_EQ(cc.get_chunk_count(), encoded.size());
  EXPECT_EQ(cc.get_chunk_size(in.length()), encoded[0].length());
 }

TEST(ErasureCodeCC, encode_RLNC)
{
  unsigned int _chunk_size = 25;
  unsigned int _data_chunk_count = 4;
  unsigned int _coded_chunk_count = 3;
  unsigned int _chunk_count = _data_chunk_count + _coded_chunk_count;

  //init
  std::string directory("directory");
  ErasureCodeProfile profile;
  profile["plugin"] = "cc";
  profile["ruleset-failure-domain"] = "osd";
  profile["k"] = std::to_string(_data_chunk_count);
  profile["m"] = std::to_string(_coded_chunk_count);;
  
  ErasureCodeCCRLNC cc(directory);
  int err = cc.init(profile, &cerr);
  EXPECT_EQ(0, err);
  
  //encode
  set<int> want_to_encode;
  map<int, bufferlist> encoded;
  bufferlist in;
  bufferptr buf(buffer::create_aligned(_chunk_size * _data_chunk_count, SIMD_ALIGN));
  std::generate_n(&buf[0], _chunk_size* _data_chunk_count, rand);
  in.push_back(std::move(buf));
  
  for (unsigned int i = 0; i < cc.get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }

  err = cc.encode(want_to_encode, in, &encoded);
  
  EXPECT_EQ(_chunk_count, cc.get_chunk_count());  
  EXPECT_EQ(0, err);
  EXPECT_EQ(cc.get_chunk_count(), encoded.size());

  //check the first k encoded chunks and compare to in
  bufferlist tmp;
  int cmp;
  for (unsigned int i = 0; i < cc.get_data_chunk_count(); ++i) {
    tmp.clear();
    EXPECT_EQ(_chunk_size, encoded[i].length());
    if (_chunk_size * (i+1) <= in.length()) {
      tmp.substr_of(in, _chunk_size * i, _chunk_size);
      cmp = memcmp(encoded[i].c_str(), tmp.c_str(), _chunk_size);
    } else {
      tmp.substr_of(in, _chunk_size * i, in.length() % _chunk_size);
      cmp = memcmp(encoded[i].c_str(), tmp.c_str(), in.length() % _chunk_size);
    }
    EXPECT_EQ(0, cmp);
  }
  //check the size of the last m encoded chunks
  for (unsigned int i = cc.get_data_chunk_count(); i < cc.get_chunk_count(); ++i) {
     EXPECT_LT(_chunk_size, encoded[i].length());
  }
 }

TEST(ErasureCodeCC, decode_RS)
{

  unsigned int _chunk_size = 64;
  unsigned int _data_chunk_count = 4;
  unsigned int _coded_chunk_count = 4;
  unsigned int _chunk_count = _data_chunk_count+ _coded_chunk_count;
   
  //init
  std::string directory("directory");
  ErasureCodeProfile profile;
  profile["plugin"] = "cc";
  profile["ruleset-failure-domain"] = "osd";
  profile["k"] = std::to_string(_data_chunk_count);
  profile["m"] = std::to_string(_coded_chunk_count);;
  
  ErasureCodeCCRS cc(directory);
  int err = cc.init(profile, &cerr);
  EXPECT_EQ(0, err);
  
  //encode
  bufferlist in;
  bufferptr buf(buffer::create_aligned(_chunk_size * _data_chunk_count, SIMD_ALIGN));
  std::generate_n(&buf[0], _chunk_size * _data_chunk_count, rand);
  in.push_back(std::move(buf));
  set<int> want_to_encode;
  map<int, bufferlist> encoded;
  for (unsigned int i = 0; i < cc.get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }
  EXPECT_EQ(_chunk_count, cc.get_chunk_count());

  err = cc.encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, err);
  EXPECT_EQ(cc.get_chunk_count(), encoded.size());
  EXPECT_EQ(cc.get_chunk_size(in.length()), encoded[0].length());

  //Save results for later comparison
  map<int, bufferlist> encoded_saved;
  for(unsigned int i =0; i< _chunk_count; i++){
    bufferptr buf(buffer::create_aligned(_chunk_size, SIMD_ALIGN));
    memcpy(&buf[0], &encoded[i][0], _chunk_size);
    encoded_saved[i].push_front(buf);
  }

  //Erase first three data packets
  encoded.erase(0);
  encoded.erase(1);
  encoded.erase(2);
  
  //decode
  int want_to_decode[_chunk_count];
  for(unsigned int i=0; i < _chunk_count; i++)
    want_to_decode[i] = i;
  map<int, bufferlist> decoded;
  decoded.clear();
  err = cc.decode(set<int>(want_to_decode, want_to_decode + _chunk_count), encoded, &decoded);
  EXPECT_EQ(0, err);
  EXPECT_EQ(_chunk_count, decoded.size());
  EXPECT_EQ(_chunk_size, decoded[0].length());

  //compare
  bufferlist tmp;
  int cmp;
  for (unsigned int i = 0; i < cc.get_data_chunk_count(); ++i) {
    tmp.clear();
    EXPECT_EQ(_chunk_size, decoded[i].length());
    if (_chunk_size * (i+1) <= in.length()) {
      tmp.substr_of(in, _chunk_size * i, _chunk_size);
      cmp = memcmp(decoded[i].c_str(), tmp.c_str(), _chunk_size);
    } else {
      tmp.substr_of(in, _chunk_size * i, in.length() % _chunk_size);
      cmp = memcmp(decoded[i].c_str(), tmp.c_str(), in.length() % _chunk_size);
    }
    EXPECT_EQ(0, cmp);
  }
  for(unsigned i = cc.get_data_chunk_count(); i < cc.get_chunk_count(); ++i){
    EXPECT_EQ(_chunk_size, decoded[i].length());
    cmp = memcmp(decoded[i].c_str(), encoded_saved[i].c_str(), _chunk_size);
    EXPECT_EQ(0, cmp);
  }
  
}


TEST(ErasureCodeCC, decode_RLNC)
{
  srand(0);
  unsigned int _chunk_size = 25;
  unsigned int _data_chunk_count = 4;
  unsigned int _coded_chunk_count = 3;
  unsigned int _chunk_count = _data_chunk_count+ _coded_chunk_count;
   
  //init
  std::string directory("directory");
  ErasureCodeProfile profile;
  profile["plugin"] = "cc";
  profile["ruleset-failure-domain"] = "osd";
  profile["k"] = std::to_string(_data_chunk_count);
  profile["m"] = std::to_string(_coded_chunk_count);;
  
  ErasureCodeCCRLNC cc(directory);
  int err = cc.init(profile, &cerr);
  EXPECT_EQ(0, err);
  
  //encode
  bufferlist in;
  bufferptr buf(buffer::create_aligned(_chunk_size * _data_chunk_count, SIMD_ALIGN));
  std::generate_n(&buf[0], _chunk_size * _data_chunk_count, rand);
  in.push_back(std::move(buf));
  set<int> want_to_encode;
  map<int, bufferlist> encoded;
  for (unsigned int i = 0; i < cc.get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }
  EXPECT_EQ(_chunk_count, cc.get_chunk_count());

  err = cc.encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, err);
  EXPECT_EQ(cc.get_chunk_count(), encoded.size());

  //Save results for later comparison
  map<int, bufferlist> encoded_saved;
  for(unsigned int i =0; i< _data_chunk_count; i++){
    bufferptr buf(buffer::create_aligned(_chunk_size, SIMD_ALIGN));
    memcpy(&buf[0], &encoded[i][0], _chunk_size);
    encoded_saved[i].push_front(buf);
  }

  for(unsigned int i =_data_chunk_count; i< _chunk_count; i++){
    bufferptr buf(buffer::create_aligned(34, SIMD_ALIGN));
    memcpy(&buf[0], &encoded[i][0], 34);
    encoded_saved[i].push_front(buf);
  }
  
  //Erase first three data packets
  encoded.erase(0);
  encoded.erase(1);
  encoded.erase(2);
  
  //decode
  int want_to_decode[_chunk_count];
  for(unsigned int i=0; i < _chunk_count; i++)
    want_to_decode[i] = i;
  map<int, bufferlist> decoded;
  decoded.clear();
  err = cc.decode(set<int>(want_to_decode, want_to_decode + _chunk_count), encoded, &decoded);
  EXPECT_EQ(0, err);
  EXPECT_EQ(_chunk_count, decoded.size());
  EXPECT_EQ(_chunk_size, decoded[0].length());

  //compare
  bufferlist tmp;
  int cmp;
  for (unsigned int i = 0; i < cc.get_data_chunk_count(); ++i) {
    tmp.clear();
    EXPECT_EQ(_chunk_size, decoded[i].length());
    if (_chunk_size * (i+1) <= in.length()) {
      tmp.substr_of(in, _chunk_size * i, _chunk_size);
      cmp = memcmp(decoded[i].c_str(), tmp.c_str(), _chunk_size);
    } else {
      tmp.substr_of(in, _chunk_size * i, in.length() % _chunk_size);
      cmp = memcmp(decoded[i].c_str(), tmp.c_str(), in.length() % _chunk_size);
    }
    EXPECT_EQ(0, cmp);
  }

  //Check that non-systematic packets are not modified
  for(unsigned i = cc.get_data_chunk_count(); i < cc.get_chunk_count(); ++i){
    EXPECT_EQ(encoded[i].length(), decoded[i].length());
    cmp = memcmp(decoded[i].c_str(), encoded_saved[i].c_str(), encoded[i].length()); 
    EXPECT_EQ(0, cmp);
  }
  
}


TEST(ErasureCodeCC, recode_RLNC)
{
  srand(0);
  unsigned int _chunk_size = 25;
  unsigned int _data_chunk_count = 4;
  unsigned int _coded_chunk_count = 3;
  unsigned int _chunk_count = _data_chunk_count + _coded_chunk_count;
   
  //init
  std::string directory("directory");
  ErasureCodeProfile profile;
  profile["plugin"] = "cc";
  profile["ruleset-failure-domain"] = "osd";
  profile["k"] = std::to_string(_data_chunk_count);
  profile["m"] = std::to_string(_coded_chunk_count);;
  
  ErasureCodeCCRLNC cc(directory);
  int err = cc.init(profile, &cerr);
  EXPECT_EQ(0, err);
  
  //encode
  bufferlist in;
  bufferptr buf(buffer::create_aligned(_chunk_size * _data_chunk_count, SIMD_ALIGN));
  std::generate_n(&buf[0], _chunk_size * _data_chunk_count, rand);
  in.push_back(std::move(buf));
  set<int> want_to_encode;
  map<int, bufferlist> encoded;
  for (unsigned int i = 0; i < cc.get_chunk_count(); ++i) {
    want_to_encode.insert(i);
  }
  EXPECT_EQ(_chunk_count, cc.get_chunk_count());

  err = cc.encode(want_to_encode, in, &encoded);
  EXPECT_EQ(0, err);
  
  //Erase three non-systematic packets
  encoded.erase(3);
  encoded.erase(4);
  encoded.erase(5);
  
  //decode
  int want_to_decode[_chunk_count];
  for(unsigned int i=0; i < _chunk_count; i++)
    want_to_decode[i] = i;
  map<int, bufferlist> decoded;
  decoded.clear();
  err = cc.decode(set<int>(want_to_decode, want_to_decode + _chunk_count), encoded, &decoded);
  EXPECT_EQ(0, err);

  //Erase three systematic packets
  decoded.erase(0);
  decoded.erase(1);
  decoded.erase(2);

  //decode and recode
  map<int, bufferlist> recoded;
  recoded.clear();
  err = cc.decode(set<int>(want_to_decode, want_to_decode + _chunk_count), decoded, &recoded);
  EXPECT_EQ(0, err);

  //compare
  bufferlist tmp;
  int cmp;
  for (unsigned int i = 0; i < cc.get_data_chunk_count(); ++i) {
    tmp.clear();
    EXPECT_EQ(_chunk_size, recoded[i].length());
    if (_chunk_size * (i+1) <= in.length()) {
      tmp.substr_of(in, _chunk_size * i, _chunk_size);
      cmp = memcmp(recoded[i].c_str(), tmp.c_str(), _chunk_size);
    } else {
      tmp.substr_of(in, _chunk_size * i, in.length() % _chunk_size);
      cmp = memcmp(recoded[i].c_str(), tmp.c_str(), in.length() % _chunk_size);
    }
    EXPECT_EQ(0, cmp);
  }
}
