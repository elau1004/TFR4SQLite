#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <libgen.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
//
#if   defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#endif


#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1  // Required by SQLite extension header.


#ifndef _TFR_H
#define _TFR_H

#ifdef   DEBUG_MODE
#define  DEBUG_APITRACE( args )  fprintf( stderr ,args );
#else
#define  DEBUG_APITRACE( args )
#endif

#ifdef   SHOW_USEDMEMORY
#undef   SHOW_USEDMEMORY
#define  SHOW_USEDMEMORY( args )  fprintf( stderr ,"%sSQLite3 Memory used:\t%lld\n\n" ,args ,sqlite3_memory_used() );
#else
#define  SHOW_USEDMEMORY( args )
#endif


// Error Messages
#define  TFR_ERRUNRECONLITERAL   "TFRERR: Unrecognize literal"


// Memory value copied from sqlite Mem struct.
#ifndef  TFR_MAX_TEXT_SIZE
#define  TFR_MAX_TEXT_SIZE 4096
#endif
typedef     struct   MemValue_  {
            unsigned char  dataType;
            sqlite_int64   i;
            double         r;
            int            n;
            char          *z;
}         __attribute__  ((aligned))
MemValue;

typedef     struct   Mapping_    {     // Mapping attributes structure.
            char          *mapFrom;          // Convert from this string.
   unsigned char           mapFromLen;       // Convert from this string length.
            char          *mapTo;            // Convert to   this string.
   unsigned char           mapToLen;         // Convert to   this string length.
}         __attribute__  ((aligned))
Mapping;


// Field Blob Type.
#define  TFR_BLOBTYPE_UNKNOWN    0
#define  TFR_BLOBTYPE_BINARY     1
#define  TFR_BLOBTYPE_HEX        2
#define  TFR_BLOBTYPE_OCTAL      3

// Field Visibility.
#define  TFR_VISIBILITY_UNKNOWN  0
#define  TFR_VISIBILITY_VISIBLE  1
#define  TFR_VISIBILITY_HIDDEN   2
#define  TFR_VISIBILITY_IGNORE   3

// Field Character.
#define  TFR_ENCODING_UNKNOWN    0
#define  TFR_ENCODING_PERCENT    1
#define  TFR_ENCODING_ENTITY     2

typedef     struct   FieldAttr_  {     // Field attribute structure.
            char          *fieldName;        // Field name declared for the virtual table column.
   unsigned char           dataType;         // Data type declared for this field.
   unsigned char           blobType;         // Input blob type encoding.
   unsigned char           orderPosition;    // The position of this field for the order of the records in the file/dataset.
   unsigned char           visibility;       // The visibility of this field defined for SQLite3 shadow table.
   unsigned short          fromPosition;     // The starting positon of the field in the record. Starting from position 1.
   unsigned short          thruPosition;     // The ending position of the field in the record.
            char          *delimiter;        // The field seperator.
   unsigned char           delimiterLen;     // Delimiter length;
            char          *leftEnclose;      // Left token that starts the enclosure of a string.
   unsigned char           leftEncloseLen;   // Left token length.
            char          *rightEnclose;     // Right token that ends  the enclosure of a string.
   unsigned char           rightEncloseLen;  // Right token length.
            char          *escString;        // Escape string to interpret the next character literally.
   unsigned char           escStringLen;     // Escape string length.
            char          *subDelimiter;     // The sub-field seperator.
   unsigned char           subDelimiterLen;  // Sub-Delimiter length.
   unsigned char           charEncoding;     // Content encoding.
   //
   unsigned char           mapCount;         // The count of data value conversion map entries.
   struct   Mapping_      *mapList;          // Data conversion mappings.
}         __attribute__  ((aligned))
FieldAttr;  // TODO: Validate string length.

// File type.
#define  TFR_FILETYPE_UNKNOWN    0
#define  TFR_FILETYPE_TEXT       1
#define  TFR_FILETYPE_LZ4        2
#define  TFR_FILETYPE_GZIP       3
#define  TFR_FILETYPE_BZ2        4
#define  TFR_FILETYPE_LZO        5

typedef     struct   FileAttr_   {     // File attribute structure.
            char          *fullName;         // Full path name of the file.
            char           fullNameLen;      // Full path name string length.
            char          *dirName;          // The path of the directory where this file resides.
            char           dirNameLen;       // The path of the directory string length.
            char          *baseName;         // The base name of the file.
            char           baseNameLen;      // The base name string length.
            char          *modifiedOn;       // Timestamp when the content was modified.
            char           modifiedOnLen;    // Timestamp string length.
           _off_t          fileSize;         // The size of the file.  Used in stat.h.
   unsigned char           fileType;         // The type of file.
            int            fileHandle;       // File handle asociated with the current opened file.
           _mode_t         statMode;         // The internal file mode.  Used in stat.h.
   struct   FileAttr_     *next;             // Pointer to the next file attribute.  Sub-directories makes it harder to pre-estimate size.
}         __attribute__  ((aligned))
FileAttr;   // TODO: Validate string length.

typedef     struct   RecBufCtrl_  {    // Record Buffer Control struct.
   unsigned short          startPos;         // Beginning position in the slab buffer to start reading from.   Max 64K (0 - 65535).
   unsigned short          length;           // The length to read from the starting position.
   unsigned char           orderPosition;    // The position of this field for the order of the records in the file/dataset.
            sqlite_int64   i;
            double         r;
   struct   FieldAttr_    *field;            // Pointer to the field attribute.
}         __attribute__  ((aligned))
RecBufCtrl;

typedef     struct   FilterColumn_  {  // Filter column.
            short          colID;            // Column ID.
   unsigned char           op;               // SQLITE3 constraints enumerator.
            MemValue       value;            // Value to constraint on.
}         __attribute__  ((aligned))
FilterColumn;

typedef     struct   FilterCtrl_ {     // Filter control structure.
   unsigned short          filterCount;      // Count of columns to apply filtering on.
            FilterColumn  *filterCol;        // Array of columns to  be contrainted on.
}         __attribute__  ((aligned))
FilterCtrl;

// Buffer sizes.
#define  TFR_RECBUFSIZE  (32*1024      )  //32 Kib
#define  TFR_FILEBUFSIZE ( 1*1024*1024 )  // 1 Mib

typedef     struct   CursorCtrl_ {     // Cursor attribute structure.
            sqlite_uint64  rowNumber;        // Global Row number.
            sqlite_uint64  recNumber;        // File Record number.
            char          *hostName;         // The name of the machine being run on.
            char           hostNameLen;      // The name of the machine string length.
   unsigned short          sdirCount;        // The count of subdirectory read.
   unsigned short          fileCount;        // The count of files to read.
   struct   FileAttr_     *fileList;         // Pointer to start of the file list.
   struct   FileAttr_     *currFile;         // Pointer to the file we are processing. If NULL we have reached the end of the dataset.
            int            currFileHandle;   // File handle asociated with the current opened file.
            size_t         readBufferSize;   // The number of bytes returned from the read() operation to populate the file buffer.
   unsigned int            fieldStartPos;    // Starting position in the buffer for the current field.
   unsigned int            fileBufferPos;    // Position in the buffer to begin the next scan.
// unsigned char           currBuffer;       // Switch indicating the currect active file buffer.
            char          *fileBuffer0;      // Pointer to an allocated block of character to be scanned for the next row and fields.
//          char          *fileBuffer1;      // Just an idea! Load the next buffer in the background while to processing the current buffer.
   unsigned short          hdrSkipped;       // Headers skipped.
   unsigned short          recSkipped;       // Records skipped.
   unsigned short          currField;        // The current field that is being scanned from the file buffer..
   unsigned char           isEndOfRec;       // The end of record boolean.
   unsigned char           isLeftEncl;       // The left opening enclose found.
   unsigned char           lftEncPos;        // The current indexed char in the left  enclosure pattern string to test against.
   unsigned char           rgtEncPos;        // The current indexed char in the right enclosure pattern string to test against.
   unsigned char           fldDelPos;        // The current indexed char in the field    delimiter string to test against.
   unsigned char           subDelPos;        // The current indexed char in the subField delimiter string to test against.
   unsigned char           recDelPos;        // The current indexed char in the record   delimiter string to test against.
   unsigned short          rbcCount;         // The count of record buffer count for the data set.
   unsigned short          currRBC;          // The current field that is being scanned into the record buffer.
   unsigned short          recSize;          // The size occupied by the record in the record buffer.
            char          *recBuffer;        // Pointer to an allocated block of character to be assemble into a row.
   unsigned short          recBufferLen;     // The size of the record buffer. Maximum size is 65535 bytes.
   struct   RecBufCtrl_   *recBufferCtrl;    // Pointer to an allocated zero based array of control data to mark start and length of fields in the slab allocated record buffer.
   struct   FilterCtrl_   *recFilterCtrl;    // Pointer to an allocated structure of column and filter operation to constraint the data set.
// struct   MemValue_     *memValue;         // Pointer to an allocated structure of memory value to reduce dynamic memory allocation.
}         __attribute__  ((aligned))
CursorCtrl; // TODO: Validate string length.

// Type of record skipping.
#define  TFR_SKIP_NONE           0
#define  TFR_SKIP_HEADER_DATASET 2  // bit 2
#define  TFR_SKIP_HEADER_PERFILE 6  // bit 3
#define  TFR_SKIP_RECORD_DATASET 8  // bit 4
#define  TFR_SKIP_RECORD_PERFILE 24 // bit 5

// Dataset sorted order
#define  TFR_ORDEREDBY_NONE      0
#define  TFR_ORDEREDBY_DATASET   1
#define  TFR_ORDEREDBY_PERFILE   2

// Character set.
#define  TFR_CHARSET_UNKNOWN     0
#define  TFR_CHARSET_ASCII       1
#define  TFR_CHARSET_EBCDIC      2
// Character Encoding.
#ifndef  TFR_ENCODING_UNKNOWN
#define  TFR_ENCODING_UNKNOWN    0
#endif
#define  TFR_ENCODING_UTF8       1
#define  TFR_ENCODING_UTF16      2
#define  TFR_ENCODING_UTF32      4

// Byte order.
#define  TFR_BYTEORDER_UNKNOWN   0
#define  TFR_BYTEORDER_BIG       1
#define  TFR_BYTEORDER_LITTLE    2

// Verbosity level.
#define  TFR_VERBOSITY_OFF       0
#define  TFR_VERBOSITY_FATAL     1  // System is unusable
#define  TFR_VERBOSITY_ALERT     2  // Action must be taken immediately
#define  TFR_VERBOSITY_CRITICAL  3  // Critical conditions
#define  TFR_VERBOSITY_ERROR     4  // Error conditions
#define  TFR_VERBOSITY_WARNING   5  // Warning conditions
#define  TFR_VERBOSITY_NOTICE    6  // Normal but significant condition
#define  TFR_VERBOSITY_INFO      7  // Informational messages
#define  TFR_VERBOSITY_DEBUG     8  // Debug-level messages
#define  TFR_VERBOSITY_TRACE     9


typedef     struct   DatasetAttr_   {  // Dataset attribute structure.  This is the root structure.
            char          *fileGlob;         // The file/dir pattern to expand.
            char          *fldDelimiter;     // Header field delimiter.
   unsigned char           fldDelimiterLen;  // Header field delimiter length;
            char          *recDelimiter;     // Header record delimiter.
   unsigned char           recDelimiterLen;  // Header record delimiter length;
   unsigned char           typeToSkip;       // A bitmap flag is instruct skipping n rows. 1=header ,2=dataset ,3=perFile
                                             // A bitmap flag is instruct skipping n rows. 0=None ,1=Header/Dataset ,2=Header/Perfile ,4=Record/dataset 8=Record/Perfile
   unsigned short          hdrsToSkip;       // Headers to skip before the start of data to read.
   unsigned short          recsToSkip;       // Records to skip before the start of data to read.
   unsigned char           orderedBy;        // The records ordered in the dataset.
   unsigned int            bufferSize;       // The size of the input file buffer.
   unsigned char           characterSet;     // Global character set for the imput files.
   unsigned char           textEncoding;     // Global text encoding for the input files.
   unsigned char           byteOrder;        // Global byte order for the files.
   unsigned char           doFiltering;      // Enable xBest() and xFilter();
   //
   unsigned short          fieldCount;       // The count of fields for the data set.
   struct   FieldAttr_    *fieldList;        // Pointer to start of the zero based field list.
}         __attribute__  ((aligned))
DatasetAttr;


//*
// TFR extended structures.
//
typedef     struct   TfrTable_ {       // TFR (Text File Reader) virtual table.
   sqlite3_vtab            base;       // Base structure - MUST be first!
   DatasetAttr            *dsa;
}  TfrTable;

typedef     struct   TfrCursor_ {      // TFR (Text File Reader) virtual table cursor.
   sqlite3_vtab_cursor     base;       // Base structure - MUST be first.  Contains: sqlite3_vtab *pVtab;
   CursorCtrl             *cursorCtrl;
}  TfrCursor;

#endif   // _TFR_H


#ifdef   SHOW_VTABDEF
//*
// Display functions.
//

// replacechar()  :  Use for debugging by formatting text for the screen.
//
static
char *replacechar( char *str ,const char old ,const char new )
{
   char  *p = str;

   while(*p )
   {
      if(*p == old)  *p = new;
      ++p;
   }

   return str;
}

static
void  displayFields( FieldAttr *pFields ,const unsigned short pSize )
{
   unsigned short i,j,k,l,m;
   char     fmtStr[64];
   char    *delStr;
   char    *encStr;

   // Figure out the longest delimiter and fieldname.
   for( i=0,j=6,k=10,l=9 ; i < pSize ; i++ )
   {
      m=(NULL  == pFields[i].delimiter  ) ? 0 : strlen( pFields[i].delimiter   );
      if(j<m) j = m;

      m=(NULL  == pFields[i].fieldName  ) ? 0 : strlen( pFields[i].fieldName   );
      if(k<m) k = m;

      if(l< pFields[i].leftEncloseLen + pFields[i].rightEncloseLen )
         l= pFields[i].leftEncloseLen + pFields[i].rightEncloseLen;
   }


   sprintf( fmtStr ," I  O Type      Bgn End %%-%ds %%-%ds %%-%ds Mapping\n"           ,j+3,k+1,l+1);  printf(  fmtStr ,"Delimiter" ,"FieldName" ,"Enclosed");
   sprintf( fmtStr ,"-- -- --------- ------- %%-%ds %%-%ds %%-%ds -------\n"           ,j+3,k+1,l+1);  printf(  fmtStr ,"---------" ,"---------" ,"--------");
   sprintf( fmtStr ,"%%2d %%2d %%1d %%-7s %%3d %%3d %%2d %%-%ds %%c%%-%ds %%-%ds %%2d" ,j  ,k  ,l  );//printf( "%s\n"  ,fmtStr );

   for( i = 0 ; i < pSize ; i++ )
   {
      delStr = sqlite3_mprintf("%s",(NULL == pFields[i].delimiter) ? "null" : pFields[i].delimiter);
      delStr = replacechar(replacechar(replacechar( delStr ,'\t' ,(char)175) ,'\n' ,(char)192) ,'\r' ,(char)212);

      encStr = sqlite3_mprintf("%s %s" ,(NULL == pFields[i].leftEnclose ) ? "" : pFields[i].leftEnclose
                                       ,(NULL == pFields[i].rightEnclose) ? "" : pFields[i].rightEnclose );

      fprintf( stderr,fmtStr  // %2d %2d %1d %3d-%3d %-7s %2d %-11s %c%-15s %2d
                     ,i
                     ,pFields[i].orderPosition
                     ,pFields[i].dataType
                     ,pFields[i].dataType    == SQLITE_INTEGER          ?  "INTEGER":
                      pFields[i].dataType    == SQLITE_FLOAT            ?  "FLOAT"  :
                      pFields[i].dataType    == SQLITE_TEXT             ?  "TEXT"   :
                      pFields[i].dataType    == SQLITE_BLOB             ?  "BLOB"   : "UNKNOWN"
                     ,pFields[i].fromPosition
                     ,pFields[i].thruPosition
                     ,pFields[i].delimiterLen
                     ,pFields[i].delimiter   == NULL                    ?  "SPACES" :  delStr
                     ,pFields[i].visibility  == TFR_VISIBILITY_HIDDEN   ?  '*'      :
                      pFields[i].visibility  == TFR_VISIBILITY_IGNORE   ?  '!'      : ' '
                     ,pFields[i].fieldName
                     ,pFields[i].leftEnclose == NULL                    ?  " "      :  encStr
                     ,pFields[i].mapCount
      );
      sqlite3_free(  delStr  );

      for( j = 0 ; j <pFields[i].mapCount ; j++ )
      {
         fprintf( stderr,"%c%s->%s "
                        ,(j==0 ? ' ' : ',')
                        ,(NULL == pFields[i].mapList[j].mapFrom) ? "(NULL)" : pFields[i].mapList[j].mapFrom
                        ,(NULL == pFields[i].mapList[j].mapTo  ) ? "(NULL)" : pFields[i].mapList[j].mapTo
         );
      }
      putchar('\n');
   }
   putchar('\n');

   fprintf( stderr," * =Hidden field.\n");
   fprintf( stderr," ! =Ignore field.\n");
}

static
void  displayDSA( DatasetAttr *pDsa )
{
   char    *fDelStr  =  sqlite3_mprintf("%s",(NULL == pDsa->fldDelimiter) ? "null" : pDsa->fldDelimiter);
   char    *rDelStr  =  sqlite3_mprintf("%s",(NULL == pDsa->recDelimiter) ? "null" : pDsa->recDelimiter);


   replacechar( fDelStr ,'\t',(char)175 );
   replacechar(
   replacechar( rDelStr ,'\n',(char)192),'\r' ,(char)212 );

   fprintf( stderr ,"DatasetAttr:  &(%p)\n"   ,pDsa               );
   fprintf( stderr ,"   fileGlob     = %s\n"  ,pDsa->fileGlob     );
   fprintf( stderr ,"   fldDelimiter = %s\n"  ,      fDelStr       );
   fprintf( stderr ,"   recDelimiter = %s\n"  ,      rDelStr      );
   fprintf( stderr ,"   typeToSkip   = %d\n"  ,pDsa->typeToSkip   );
   fprintf( stderr ,"   hdrsToSkip   = %d\n"  ,pDsa->hdrsToSkip   );
   fprintf( stderr ,"   recsToSkip   = %d\n"  ,pDsa->recsToSkip   );
   fprintf( stderr ,"   orderedBy    = %d\n"  ,pDsa->orderedBy    );
   fprintf( stderr ,"   characterSet = %d\n"  ,pDsa->characterSet );
   fprintf( stderr ,"   textEncoding = %d\n"  ,pDsa->textEncoding );
   fprintf( stderr ,"   byteOrder    = %d\n"  ,pDsa->byteOrder    );
   fprintf( stderr ,"   bufferSize   = %d\n"  ,pDsa->bufferSize   );
   fprintf( stderr ,"      fieldCount= %d\n"  ,pDsa->fieldCount   );

   sqlite3_free(  fDelStr );
   sqlite3_free(  rDelStr );

   displayFields( pDsa->fieldList   ,pDsa->fieldCount );
}

static
void  displayVTable( sqlite3_vtab *pVTable )
{
   fprintf( stderr ,"sqlite3_vtab: &(%p)\n"  ,pVTable          );
   fprintf( stderr ,"   zErrMsg      =%s\n"  ,pVTable->zErrMsg );

   TfrTable *table   = (TfrTable *) pVTable;

   if(NULL  != table->dsa) displayDSA( table->dsa );
}

static
void  displayFiles(  FileAttr *pFileList )
{
   unsigned  short   i= 0;
   FileAttr *currFile = pFileList;

   fprintf( stderr ," I  Pointer T             Modified On       Size FileName\n");
   fprintf( stderr ,"-- -------- - ----------------------- ---------- --------\n");

   while(NULL != currFile )
   {
      fprintf( stderr ,"%2d %p %1d %s %10d %s (%s %s)\n",  ++i ,currFile ,currFile->fileType ,currFile->modifiedOn ,currFile->fileSize ,currFile->fullName ,currFile->dirName ,currFile->baseName );

      currFile = currFile->next;
   }
   putchar('\n');
}

static
void  displayVCursor(  sqlite3_vtab_cursor *pVCursor  )
{
   fprintf( stderr ,"sqlite3_vtab_cursor: &(%p)\n"  ,pVCursor   );

   TfrCursor *cursor  = (TfrCursor *)pVCursor;

// if(NULL  != cursor->dsa       )  displayDSA(        cursor->dsa         );
// if(NULL  != cursor->cursorCtrl)  displayCursorCtrl( cursor->cursorCtrl  );
}

static
char *substr( const char *src ,const int pos ,const int len );

static
void  displayRBCFields( RecBufCtrl *pRBC ,const unsigned short size ,char *recBuffer )
{
unsigned short i;

   fprintf( stderr ,"RecBufCtrl:   &(%p)\n"  ,pRBC  );

   fprintf( stderr ," I  O Pos Len FieldName\n");
   fprintf( stderr ,"-- -- --- --- ---------\n");
   for( i = 0 ; i < size ; i++ )
   {
            char       *txtVal   =  NULL;
   unsigned short       startPos;      // Beginning position in the slab buffer to start reading from.   Max 64K (0 - 65535).
   unsigned short       length;        // The length to read from the starting position.
   unsigned char        orderPosition; // The position of this field for the order of the records in the file/dataset.

      txtVal = substr( recBuffer ,pRBC[i].startPos ,pRBC[i].length );

      fprintf( stderr ,"%2d %2d %3d %3d %-16s %s\n" ,i ,pRBC[i].orderPosition ,pRBC[i].startPos ,pRBC[i].length ,pRBC[i].field->fieldName ,txtVal );

      free(    txtVal );
   }
}

static
void  displayCursorCtrl( CursorCtrl *pCursorCtrl )
{
   fprintf( stderr ,"CursorCtrl:   &(%p)\n"   ,pCursorCtrl         );

   fprintf( stderr ,"   rowNumber   = %d\n"   ,pCursorCtrl->rowNumber          );
   fprintf( stderr ,"   recNumber   = %d\n"   ,pCursorCtrl->recNumber          );
   fprintf( stderr ,"   recBufferCtl=&%p\n"   ,pCursorCtrl->recBufferCtrl      );
   fprintf( stderr ,"   fileBuffer  =&%p\n"   ,pCursorCtrl->fileBuffer0        );
   fprintf( stderr ,"   fileList    =&%p\n"   ,pCursorCtrl->fileList           );
   fprintf( stderr ,"   currFile    =&%p\n"   ,pCursorCtrl->currFile           );
   fprintf( stderr ,"   currFileHndl= %d\n"   ,pCursorCtrl->currFileHandle     );
   fprintf( stderr ,"   currFileSize= %d\n"   ,pCursorCtrl->currFile->fileSize );
   fprintf( stderr ,"   currFileName= %s\n"   ,pCursorCtrl->currFile->fullName );
   fprintf( stderr ,"      fileCount= %d\n"   ,pCursorCtrl->fileCount          );

   if(NULL != pCursorCtrl->fileList) displayFiles( pCursorCtrl->fileList );

   fprintf( stderr ,"   rbcCount    = %d\n"               ,pCursorCtrl->rbcCount  );
   displayRBCFields( pCursorCtrl->recBufferCtrl ,pCursorCtrl->rbcCount ,pCursorCtrl->recBuffer  );
}

#endif   // SHOW_VTABDEF


//*
// Helper functions.
//*

static
void  showMemoryUsed()
{
   fprintf( stderr ,"SQLite3 Memory used:\t%lld\n" ,sqlite3_memory_used());

   #if   defined(_WIN32) || defined(_WIN64)
   MEMORYSTATUSEX             memStatEx;
   memStatEx.dwLength=sizeof( memStatEx );
   GlobalMemoryStatusEx(     &memStatEx );
   fprintf( stderr ,"Windows Memory free:\t%lld\t%2d%%\n\n" ,memStatEx.ullAvailPhys ,100-memStatEx.dwMemoryLoad );
   #endif
}

// substr() :  Return a portion of a string from source "src" starting at position "pos" for the length of "len".
//             Copied from http://www.programmingsimplified.com/c/source-code/c-substring
//
static
char *substr( const char *src ,const int pos ,const int len )
{
   int   c = 0;
   char *p;

   if(NULL ==( p = malloc( len+1 )) )
   {
      fprintf( stderr ,"substr() : Unable to allocate memory.\n");
      exit(EXIT_FAILURE);
   }

   for( ; c < len ; c++ )
   {
      *(p+c) = *(src +pos +c);
   }

   *(p+c) = '\0';    // Terminate the string.

   return   p;       // Remember to free this allocated memory.
}

// substrdecode() :  Return a portion of a decoded string from source "src" starting at position "pos" for the length of "len".
//
static    inline     // ISO C99
char *substrdecode( const char *src ,const int pos ,const int len ,const unsigned char pCharEncoding )
{
   register int   i,j,k;
   register
   unsigned char  c;
   register
   unsigned short pos1,pos2;
   char          *txtVal;

   if(TFR_ENCODING_UNKNOWN == pCharEncoding)
   {
      txtVal= substr( src ,pos ,len );
   }
   else{
      if(NULL ==( txtVal = malloc( len+1 )) )
      {
         fprintf( stderr ,"substrdecode() : Unable to allocate memory.\n");
         exit(EXIT_FAILURE);
      }

      for( i=0,k=0 ; i<len ; i++)
      {
         switch( pCharEncoding )
         {
            case  TFR_ENCODING_PERCENT:
                  switch( src[ pos +i ] )
                  {
                     case  '+':
                           txtVal[ k++ ]  =  ' ';
                           break;
                     case  '%':
                        pos1 = pos+i+1;
                        pos2 = pos+i+2;

                        if( i+2 <  len
                        &&(('0' <= src[ pos1 ] && '9' >= src[ pos1 ]) || ('A' <= src[ pos1 ] && 'F' >= src[ pos1 ]) || ('a' <= src[ pos1 ] && 'f' >= src[ pos1 ]))
                        &&(('0' <= src[ pos2 ] && '9' >= src[ pos2 ]) || ('A' <= src[ pos2 ] && 'F' >= src[ pos2 ]) || ('a' <= src[ pos2 ] && 'f' >= src[ pos2 ]))
                        ){
                           if('9' >= src[ pos1 ])  c  =  (src[ pos1 ] & 0x0F) << 4;
                           if('9' >= src[ pos2 ])  c |=  (src[ pos2 ] & 0x0F) ;
                           else                    c |= ((src[ pos2 ] & 0x0F) +9);

                           txtVal[ k++ ]  =c;
                           i += 2;
                           break;
                        }
                        // Drop through.
                     default:
                           txtVal[ k++ ]  =  src[ pos +i ];
                  }
                  break;
            case  TFR_ENCODING_ENTITY:
                  switch( src[ pos +i ] )
                  {
                     case  '&':
                        // Well form lowercase encoding.
                        if(  (i+5 <  len  && ';' == src[ pos +i+5 ]) // &nbsp; &quot; &apos;
                           ||(i+4 <  len  && ';' == src[ pos +i+4 ]) // &amp;
                           ||(i+3 <  len  && ';' == src[ pos +i+3 ]) // &lt; &gt;
                        ){
                           if(i+5 <  len  )
                           {
                              if('n' == src[ pos +i+1 ] && 'b' == src[ pos +i+2 ] && 's' == src[ pos +i+3 ] && 'p' == src[ pos +i+4 ])  {  txtVal[ k++ ] = ' ' ; i += 5; break; }
                              if('q' == src[ pos +i+1 ] && 'u' == src[ pos +i+2 ] && 'o' == src[ pos +i+3 ] && 't' == src[ pos +i+4 ])  {  txtVal[ k++ ] = '\"'; i += 5; break; }
                              if('a' == src[ pos +i+1 ] && 'p' == src[ pos +i+2 ] && 'o' == src[ pos +i+3 ] && 's' == src[ pos +i+4 ])  {  txtVal[ k++ ] = '\''; i += 5; break; }
                           }
                           if(i+4 <  len  )
                           {
                              if('a' == src[ pos +i+1 ] && 'm' == src[ pos +i+2 ] && 'p' == src[ pos +i+3 ])   {  txtVal[ k++ ] = '&'; i += 4; break; }
                           }
                           if(i+3 <  len  )
                           {
                              if('l' == src[ pos +i+1 ] && 't' == src[ pos +i+2 ])  {  txtVal[ k++ ] = '<'; i += 2; break; }
                              if('g' == src[ pos +i+1 ] && 't' == src[ pos +i+2 ])  {  txtVal[ k++ ] = '>'; i += 2; break; }
                           }
                        }
                        // Drop through.
                     default:
                        txtVal[ k++ ]=  src[ pos +i ];
                  }
                  break;
            default:
               txtVal[ k++ ]=  src[ pos +i ];
               break;
         }  // switch
      }  // for

      txtVal[k] = '\0';
   }  // else

   return   txtVal;  // Remember to free this allocated memory.
}

// uescstr() : Return a string with all the escape character converted on binary value.
//             http://en.cppreference.com/w/cpp/language/escape
//
static
char *uescstr( const char *src )
{
   int   c = 0;
   int   i = 0;
   char *p;

   for( c = 0 ; src[c] != '\0' ; c++ ) {}

   if(NULL == (p = malloc( c+1 )) )
   {
      fprintf( stderr ,"uescstr() : Unable to allocate memory.\n");
      exit(EXIT_FAILURE);
   }

   for( c = 0 ; src[c] != '\0' ; c++ )
   {
      if(src[c] == '\\' && src[c+1] != '\0' )
      {
         switch( src[c+1] )
         {
            case  'f':  p[c-i] = '\f'; ++i;  ++c;
                        break;
            case  't':  p[c-i] = '\t'; ++i;  ++c;
                        break;
            case  'n':  p[c-i] = '\n'; ++i;  ++c;
                        break;
            case  'r':  p[c-i] = '\r'; ++i;  ++c;
                        break;
            default  :  p[c-i] = src[c];
                        break;
         }
      }else             p[c-i] = src[c];
   }

   p[c-i] = '\0'; // Terminate the string.

   return   p;    // Remember to free this allocated memory.
}


static const char VTABLECREATE[] = "CREATE   TABLE  _tablename(" // SQLite internally ignore this table name for virtual table declaration.
"\n\t RecNum_     INT   HIDDEN"  // 0) Unique Record number within the file.
"\n\t,RecSize_    INT   HIDDEN"  // 1) Raw record size in buffer.
"\n\t,FileSize_   INT   HIDDEN"  // 2) File size.
"\n\t,FileDate_   TEXT  HIDDEN"  // 3) File creation timestamp.
"\n\t,FileName_   TEXT  HIDDEN"  // 4) File name.
"\n\t,FileDir_    TEXT  HIDDEN"  // 5) File directory.
"\n\t,HostName_   TEXT  HIDDEN"  // 6) Host server.
#define  TFR_METACOL_COUNT 7
;

static
char  *getVTableDDL(  sqlite3_vtab *pVTable  )
{
   register
   int       i;
   int       strLen  =  0;
   int       strMax  =  10;
   int       memMax  =  2048;
   char     *sqlStr  =  NULL;
   char     *colFmt  =  NULL;
   char     *colDef  =  NULL;
   TfrTable *tfrTable= (TfrTable *)pVTable;

   sqlStr = sqlite3_malloc( memMax );
   memset(  sqlStr ,'\0'  , memMax );
   sqlStr = strcat( sqlStr, VTABLECREATE );

   // I just like to have my columns well aligned!  Figure out the longest field name length.
   for( i = 0 ; i < tfrTable->dsa->fieldCount ; i++ )
   {
      if(TFR_VISIBILITY_IGNORE != tfrTable->dsa->fieldList[i].visibility )
      {
         strLen = strlen( tfrTable->dsa->fieldList[i].fieldName );
         if( strLen > strMax )   strMax = strLen;
      }
   }

   strMax=((strMax/3)+1)*3;   // Round up to the next increment of 3 spaces.
   // Init the string format for the column defintion to be printed out in.
   colFmt=  sqlite3_mprintf("%c%c,%%-%ds%%-6s%%s" ,10 ,9 ,strMax );

   for( i = 0 ; i < tfrTable->dsa->fieldCount ; i++ )
   {
      if(TFR_VISIBILITY_IGNORE  != tfrTable->dsa->fieldList[i].visibility )
      {
         colDef = sqlite3_mprintf( colFmt
                                 , tfrTable->dsa->fieldList[i].fieldName
                                 , tfrTable->dsa->fieldList[i].dataType   == SQLITE_INTEGER        ? "INT   " :
                                   tfrTable->dsa->fieldList[i].dataType   == SQLITE_FLOAT          ? "FLOAT " :
                                   tfrTable->dsa->fieldList[i].dataType   == SQLITE_TEXT           ? "TEXT  " :
                                   tfrTable->dsa->fieldList[i].dataType   == SQLITE_BLOB           ? "BLOB  " : ""
                                 , tfrTable->dsa->fieldList[i].visibility == TFR_VISIBILITY_HIDDEN ? "HIDDEN" : ""  );

         if(memMax < (strlen(sqlStr) + strlen(colDef) +1))
         {
            memMax += 512;
            sqlite3_realloc( sqlStr ,memMax );
         }

         sqlStr = strcat( sqlStr , colDef );
         sqlite3_free(    colDef );

      }
   }

   sqlStr= strcat(sqlStr ,"\n)" );
   sqlite3_free(  colFmt );

   return   sqlStr;  // Remember to free this allocated memory.
}


#ifndef _TFR_PARSER_H
#define _TFR_PARSER_H
//
// Parser section.
//
#define  PARSE_DATASETATTRIB  0
#define  PARSE_FIELDATTRIB    1
//
#define  TOKEN_ILLEGAL        0
#define  TOKEN_ENDOFSTRING    1
#define  TOKEN_SPACE          2
#define  TOKEN_LITERAL        3
#define  TOKEN_INTEGER        4
#define  TOKEN_STRING         5
#define  TOKEN_DASH           6
#define  TOKEN_COLON          7
#define  TOKEN_LPARENTHESES   8
#define  TOKEN_RPARENTHESES   9
//
#define  TOKEN_NONE           10
#define  TOKEN_HEADER         11
#define  TOKEN_DATASET        12
#define  TOKEN_PERFILE        13
#define  TOKEN_FIELD          14
#define  TOKEN_RECORD         15
#define  TOKEN_TERMINATED     16
#define  TOKEN_SUBTERMINATED  17
#define  TOKEN_DELIMITER      16
#define  TOKEN_SUBDELIMITER   17
#define  TOKEN_SKIP           18
#define  TOKEN_ORDERED        19
#define  TOKEN_BY             20
#define  TOKEN_CHARSET        21
#define  TOKEN_ASCII          22
#define  TOKEN_EBCDIC         23
#define  TOKEN_ENCODING       24
#define  TOKEN_UTF8           25
#define  TOKEN_UTF16          26
#define  TOKEN_UTF16BE        27
#define  TOKEN_UTF16LE        28
#define  TOKEN_UTF32          29
#define  TOKEN_UTF32BE        30
#define  TOKEN_UTF32LE        31
#define  TOKEN_BYTEORDER      32
#define  TOKEN_BIG            33
#define  TOKEN_LITTLE         34
#define  TOKEN_BUFFERSIZE     35
#define  TOKEN_KiB            36
#define  TOKEN_MiB            37
#define  TOKEN_GiB            38
//
#define  TOKEN_TEXT           50
#define  TOKEN_VARCHAR        51
#define  TOKEN_INT            52
#define  TOKEN_FLOAT          53
#define  TOKEN_BLOB           54
#define  TOKEN_BINARY         55
#define  TOKEN_HEXADECIMAL    56
#define  TOKEN_OCTAL          57
#define  TOKEN_BCD            58
#define  TOKEN_OPTIONALLY     59
#define  TOKEN_ENCLOSED       60
#define  TOKEN_ESCAPE         61
#define  TOKEN_WITH           62
#define  TOKEN_AND            64
#define  TOKEN_PERCENT        65
#define  TOKEN_ENTITY         66
#define  TOKEN_POSITION       67
#define  TOKEN_ORDER          68
#define  TOKEN_AT             69
#define  TOKEN_CONVERT        70
#define  TOKEN_FROM           71
#define  TOKEN_TO             72
#define  TOKEN_NULL           73
#define  TOKEN_HIDDEN         74
#define  TOKEN_IGNORE         75
#endif

static   char  DEBUG_ = 0;

static   unsigned
char  tfrTokenizer( const char *pzStr ,unsigned short *pTokenLen )
{
   unsigned short i=0;
   unsigned char  quote;   // Char to quote a string.
   unsigned char  vTokenID =  TOKEN_ILLEGAL;

   switch  ( *pzStr )
   {
      case  '\0':   *pTokenLen=  0;
                     return      TOKEN_ENDOFSTRING;
      case  ' ' :
      case  '\f':
      case  '\t':
      case  '\n':
      case  '\r':    for( i=1 ; '\0' != pzStr[i] &&(pzStr[i] == ' ' || pzStr[i] == '\f' || pzStr[i] == '\t' || pzStr[i] == '\n' || pzStr[i] == '\r') ; i++ ){}
                    *pTokenLen=  i;
                     return      TOKEN_SPACE;
      case  '-' :   *pTokenLen=  1;
                     return      TOKEN_DASH;
      case  ':' :   *pTokenLen=  1;
                     return      TOKEN_COLON;
      case  '(' :   *pTokenLen=  1;
                     return      TOKEN_LPARENTHESES;
      case  ')' :   *pTokenLen=  1;
                     return      TOKEN_RPARENTHESES;
      case  '\'':
      case  '\"':    quote = pzStr[0];
                     for( i= 1 ; '\0' != pzStr[i] && pzStr[i] != quote ; i++ ){}
                    *pTokenLen=  i+1;

                     if( pzStr[i] == quote ) return   TOKEN_STRING;
                     else                    return   TOKEN_ILLEGAL;
      case  '0':
      case  '1':
      case  '2':
      case  '3':
      case  '4':
      case  '5':
      case  '6':
      case  '7':
      case  '8':
      case  '9':     for( i=1 ; '\0' != pzStr[i] && (48 <= pzStr[i] && pzStr[i] <= 57) ; i++ ){/* Loop if it is char between '0' and '9' */}
                    *pTokenLen=  i;
                     return      TOKEN_INTEGER;

      default:       for( i=0 ; '\0' != pzStr[i] &&(  ('0' <= pzStr[i] && pzStr[i] <= '9' )  // 0 .. 9
                                                   || ('A' <= pzStr[i] && pzStr[i] <= 'Z' )  // A .. Z
                                                   || ('a' <= pzStr[i] && pzStr[i] <= 'z' )  // a .. z
                                                   ||  '.' == pzStr[i]
                                                   ||  '*' == pzStr[i]
                                                   ||  '?' == pzStr[i]
                                                   ||  ':' == pzStr[i]
                                                   ||  '-' == pzStr[i]
                                                   ||  '_' == pzStr[i]
                                                   ||  '/' == pzStr[i]
                                                   ||  '\\'== pzStr[i]
                                                   )
                              ; i++ ){/* Loop if it is char ,digit, lowercase, uppercase or certain punctuation. */}

                    *pTokenLen=  i;
                     if( '\0' == pzStr[i] || ' ' == pzStr[i] || '\f' == pzStr[i] || '\t' == pzStr[i] || '\n' == pzStr[i] || '\r' == pzStr[i] || '(' == pzStr[i] || ')' == pzStr[i]  )
                     {
                        char *vLiteral = substr( pzStr ,0 ,*pTokenLen );   // TODO: Convert to stack variable.
                        // TODO: Convert the following conditions to a hash look-up.
                             if( strcasecmp( vLiteral ,"PERFILE"        ) == 0 ) vTokenID =  TOKEN_PERFILE;
                        else if( strcasecmp( vLiteral ,"DATASET"        ) == 0 ) vTokenID =  TOKEN_DATASET;
                        else if( strcasecmp( vLiteral ,"HEADER"         ) == 0 ) vTokenID =  TOKEN_HEADER;
                        else if( strcasecmp( vLiteral ,"SKIP"           ) == 0 ) vTokenID =  TOKEN_SKIP;
                        else if( strcasecmp( vLiteral ,"FIELD"          ) == 0 ) vTokenID =  TOKEN_FIELD;
                        else if( strcasecmp( vLiteral ,"FIELDS"         ) == 0 ) vTokenID =  TOKEN_FIELD;
                        else if( strcasecmp( vLiteral ,"RECORD"         ) == 0 ) vTokenID =  TOKEN_RECORD;
                        else if( strcasecmp( vLiteral ,"ORDERED"        ) == 0 ) vTokenID =  TOKEN_ORDERED;
                        else if( strcasecmp( vLiteral ,"BY"             ) == 0 ) vTokenID =  TOKEN_BY;
                        else if( strcasecmp( vLiteral ,"CHARSET"        ) == 0 ) vTokenID =  TOKEN_CHARSET;
                        else if( strcasecmp( vLiteral ,"CHARACTERSET"   ) == 0 ) vTokenID =  TOKEN_CHARSET;
                        else if( strcasecmp( vLiteral ,"ASCII"          ) == 0 ) vTokenID =  TOKEN_ASCII;
                        else if( strcasecmp( vLiteral ,"EBCDIC"         ) == 0 ) vTokenID =  TOKEN_EBCDIC;
                        else if( strcasecmp( vLiteral ,"ENCODING"       ) == 0 ) vTokenID =  TOKEN_ENCODING;
                        else if( strcasecmp( vLiteral ,"UTF8"           ) == 0 ) vTokenID =  TOKEN_UTF8;
                        else if( strcasecmp( vLiteral ,"UTF16"          ) == 0 ) vTokenID =  TOKEN_UTF16;
                        else if( strcasecmp( vLiteral ,"UTF16BE"        ) == 0 ) vTokenID =  TOKEN_UTF16BE;
                        else if( strcasecmp( vLiteral ,"UTF16LE"        ) == 0 ) vTokenID =  TOKEN_UTF16LE;
                        else if( strcasecmp( vLiteral ,"UTF32"          ) == 0 ) vTokenID =  TOKEN_UTF32;
                        else if( strcasecmp( vLiteral ,"UTF32BE"        ) == 0 ) vTokenID =  TOKEN_UTF32BE;
                        else if( strcasecmp( vLiteral ,"UTF32LE"        ) == 0 ) vTokenID =  TOKEN_UTF32LE;
                        else if( strcasecmp( vLiteral ,"BYTEORDER"      ) == 0 ) vTokenID =  TOKEN_BYTEORDER;
                        else if( strcasecmp( vLiteral ,"BIG"            ) == 0 ) vTokenID =  TOKEN_BIG;
                        else if( strcasecmp( vLiteral ,"LITTLE"         ) == 0 ) vTokenID =  TOKEN_LITTLE;
                        else if( strcasecmp( vLiteral ,"BUFFERSIZE"     ) == 0 ) vTokenID =  TOKEN_BUFFERSIZE;
                        else if( strcasecmp( vLiteral ,"K"              ) == 0 ) vTokenID =  TOKEN_KiB;
                        else if( strcasecmp( vLiteral ,"KB"             ) == 0 ) vTokenID =  TOKEN_KiB;
                        else if( strcasecmp( vLiteral ,"M"              ) == 0 ) vTokenID =  TOKEN_MiB;
                        else if( strcasecmp( vLiteral ,"MB"             ) == 0 ) vTokenID =  TOKEN_MiB;
                        else if( strcasecmp( vLiteral ,"G"              ) == 0 ) vTokenID =  TOKEN_GiB;
                        else if( strcasecmp( vLiteral ,"GB"             ) == 0 ) vTokenID =  TOKEN_GiB;
                        else if( strcasecmp( vLiteral ,"TEXT"           ) == 0 ) vTokenID =  TOKEN_TEXT;
                        else if( strcasecmp( vLiteral ,"VARCHAR"        ) == 0 ) vTokenID =  TOKEN_TEXT;
                        else if( strcasecmp( vLiteral ,"INT"            ) == 0 ) vTokenID =  TOKEN_INT;
                        else if( strcasecmp( vLiteral ,"INTEGER"        ) == 0 ) vTokenID =  TOKEN_INT;
                        else if( strcasecmp( vLiteral ,"BCD"            ) == 0 ) vTokenID =  TOKEN_BCD;
                        else if( strcasecmp( vLiteral ,"REAL"           ) == 0 ) vTokenID =  TOKEN_FLOAT;
                        else if( strcasecmp( vLiteral ,"FLOAT"          ) == 0 ) vTokenID =  TOKEN_FLOAT;
                        else if( strcasecmp( vLiteral ,"BLOB"           ) == 0 ) vTokenID =  TOKEN_BLOB;
                        else if( strcasecmp( vLiteral ,"HEXADECIMAL"    ) == 0 ) vTokenID =  TOKEN_HEXADECIMAL;
                        else if( strcasecmp( vLiteral ,"HEX"            ) == 0 ) vTokenID =  TOKEN_HEXADECIMAL;
                        else if( strcasecmp( vLiteral ,"OCTAL"          ) == 0 ) vTokenID =  TOKEN_OCTAL;
                        else if( strcasecmp( vLiteral ,"BINARY"         ) == 0 ) vTokenID =  TOKEN_BINARY;
                        else if( strcasecmp( vLiteral ,"TERMINATED"     ) == 0 ) vTokenID =  TOKEN_TERMINATED;
                        else if( strcasecmp( vLiteral ,"DELIMETED"      ) == 0 ) vTokenID =  TOKEN_DELIMITER;
                        else if( strcasecmp( vLiteral ,"DELIMETER"      ) == 0 ) vTokenID =  TOKEN_DELIMITER;
                        else if( strcasecmp( vLiteral ,"DELIMITER"      ) == 0 ) vTokenID =  TOKEN_DELIMITER;
                        else if( strcasecmp( vLiteral ,"SUBTERMINATED"  ) == 0 ) vTokenID =  TOKEN_SUBTERMINATED;
                        else if( strcasecmp( vLiteral ,"SUBDELIMETED"   ) == 0 ) vTokenID =  TOKEN_SUBDELIMITER;
                        else if( strcasecmp( vLiteral ,"SUBDELIMETER"   ) == 0 ) vTokenID =  TOKEN_SUBDELIMITER;
                        else if( strcasecmp( vLiteral ,"SUBDELIMITER"   ) == 0 ) vTokenID =  TOKEN_SUBDELIMITER;
                        else if( strcasecmp( vLiteral ,"SPACE"          ) == 0 ) vTokenID =  TOKEN_SPACE;
                        else if( strcasecmp( vLiteral ,"SPACES"         ) == 0 ) vTokenID =  TOKEN_SPACE;
                        else if( strcasecmp( vLiteral ,"OPTIONALLY"     ) == 0 ) vTokenID =  TOKEN_OPTIONALLY;
                        else if( strcasecmp( vLiteral ,"ENCLOSED"       ) == 0 ) vTokenID =  TOKEN_ENCLOSED;
                        else if( strcasecmp( vLiteral ,"AND"            ) == 0 ) vTokenID =  TOKEN_AND;
                        else if( strcasecmp( vLiteral ,"POSITION"       ) == 0 ) vTokenID =  TOKEN_POSITION;
                        else if( strcasecmp( vLiteral ,"ORDER"          ) == 0 ) vTokenID =  TOKEN_ORDER;
                        else if( strcasecmp( vLiteral ,"PERCENT"        ) == 0 ) vTokenID =  TOKEN_PERCENT;
                        else if( strcasecmp( vLiteral ,"ENTITY"         ) == 0 ) vTokenID =  TOKEN_ENTITY;
                        else if( strcasecmp( vLiteral ,"HIDDEN"         ) == 0 ) vTokenID =  TOKEN_HIDDEN;
                        else if( strcasecmp( vLiteral ,"IGNORE"         ) == 0 ) vTokenID =  TOKEN_IGNORE;
                        else if( strcasecmp( vLiteral ,"CONVERT"        ) == 0 ) vTokenID =  TOKEN_CONVERT;
                        else if( strcasecmp( vLiteral ,"MAP"            ) == 0 ) vTokenID =  TOKEN_CONVERT;
                        else if( strcasecmp( vLiteral ,"FROM"           ) == 0 ) vTokenID =  TOKEN_FROM;
                        else if( strcasecmp( vLiteral ,"TO"             ) == 0 ) vTokenID =  TOKEN_TO;
                        else if( strcasecmp( vLiteral ,"NULL"           ) == 0 ) vTokenID =  TOKEN_NULL;
                        else                                                     vTokenID =  TOKEN_LITERAL;

                        free(    vLiteral );
                        return   vTokenID;
                     }
                     else
                        return   TOKEN_ILLEGAL;
   }
}

// Return the tokenID found in the beginning of input string pointed by the startPos along with the starting position and the length of the found token.
//
static   unsigned
char  tfrGetTokenID( const char *pzStr ,unsigned short *pStartPos ,unsigned short *pTokenLen )
{
   unsigned char  vTokenID =  TOKEN_ILLEGAL;
   unsigned short vTokenLen=  0;
   unsigned short vTokenPos=  0;

   vTokenID  = tfrTokenizer( pzStr+vTokenPos ,&vTokenLen );

   if( TOKEN_SPACE == vTokenID )
   {
      vTokenPos+= vTokenLen;
      vTokenID  = tfrTokenizer( pzStr+vTokenPos ,&vTokenLen );
   }

  *pStartPos = vTokenPos;
  *pTokenLen = vTokenLen;

   return      vTokenID;
}


// Populate the dataset attribute structure.
//
// TODO: Assign error message into pzErrMsg.
//       Convert to strtol() from atoi().
//       Use goto local syntaxErr label
//       Display location of error for user friendliness.
//
static int
tfrInitDSA( DatasetAttr *dsa ,char *pzStr ,char *pzErrMsg )
{
            int   i,rc        =  SQLITE_OK;
            char *vzStr       =  pzStr;
            char *vToken      =  NULL;
            char *vTmp        =  NULL;
   unsigned char  vTokenID    =  TOKEN_NONE;
   unsigned char  vTokenTmp   =  TOKEN_NONE; // Temp token
   unsigned char  vSyntxSeg   =  TOKEN_NONE; // Current syntax segment
   unsigned short vInputLen   =  0;
   unsigned short vInputPos   =  0;
   unsigned short vTokenLen   =  0;
   unsigned short vTokenPos   =  0;
   unsigned short vErrorPos   =  0;
   unsigned short vPrevPos    =  0;
   // Look ahead one token for a LITERAL that MUST be "PERFILE".
   unsigned short vLen        =  0;
   unsigned short vPos        =  0;

   vInputLen   =  strlen(pzStr);

   // Set the default.
   dsa->bufferSize      =  TFR_FILEBUFSIZE;
   dsa->characterSet    =  TFR_CHARSET_ASCII;
// dsa->recDelimiter    =  sqlite3_mprintf("\n");
// dsa->recDelimiterLen =  1;

   do{
      switch ( vTokenID )
      {
         case  TOKEN_NONE:
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
               switch ( vTokenID )
               {
                  case  TOKEN_LITERAL:
                        vToken         =  substr( vzStr  ,vTokenPos  ,vTokenLen  );
                        dsa->fileGlob  =  sqlite3_mprintf(vToken  );
                        free( vToken );
                        break;
                  case  TOKEN_STRING:
                        vToken         =  substr( vzStr  ,vTokenPos+1,vTokenLen-2);
                        dsa->fileGlob  =  sqlite3_mprintf(vToken  );
                        free( vToken );
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting the file glob pattern.\n");
                        break;
               }
               break;
         case  TOKEN_HEADER:
               vSyntxSeg=  vTokenID;
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);

               switch ( vTokenID )
               {
                  case  TOKEN_INTEGER:
                        vToken         =  substr(  vzStr ,vTokenPos ,vTokenLen );
                        dsa->hdrsToSkip=  atoi( vToken );
                        free( vToken );

                        vPos     =  0;
                        vTokenID =  tfrGetTokenID( vzStr+(vTokenPos+vTokenLen) ,&vPos ,&vLen );  // Look ahead one token.

                        dsa->typeToSkip= dsa->typeToSkip | TFR_SKIP_HEADER_DATASET;

                        switch ( vTokenID )
                        {
                           case  TOKEN_PERFILE:
//                               dsa->typeToSkip= dsa->typeToSkip |(TFR_SKIPTYPE_PERFILE << TFR_SKIPTYPE_HEADER);
                                 dsa->typeToSkip= dsa->typeToSkip | TFR_SKIP_HEADER_PERFILE;
                           case  TOKEN_DATASET:
                                 vzStr   +=  vTokenPos+vTokenLen; // Look ahead is valid therefore advance it.
                                 vTokenPos = vPos;
                                 vTokenLen = vLen;
//                         case  TOKEN_SKIP:
//                         case  TOKEN_FIELD:
//                         case  TOKEN_RECORD:
//                         case  TOKEN_ENDOFSTRING:   // Done with header spec, default the rest!
//                               break;
//                         default:
//                               rc = SQLITE_ERROR;
//                               fprintf( stderr ,"TFRERR: Expecting DATASET or PERFILE keyword for HEADER directive.\n");
                                 break;
                        }
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting an integer value of headers to skip.\n");
                        break;
               }
               break;
         case  TOKEN_FIELD:
         case  TOKEN_RECORD:
#if 0
            if(TOKEN_HEADER == vSyntxSeg) {
#endif
               vTokenTmp=  vTokenID;
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);

               switch ( vTokenID )
               {
                  case  TOKEN_DELIMITER:
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenPos=  0;
                        vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);

                        switch ( vTokenID )
                        {
                           case  TOKEN_BY:
                                 vzStr   +=  vTokenPos+vTokenLen;
                                 vTokenPos=  0;
                                 vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
                           default:
                                 break;
                        }
                        switch ( vTokenID )
                        {
                           case  TOKEN_STRING:
                                 vTmp           =  substr( vzStr  ,vTokenPos+1,vTokenLen-2);
                                 vToken         =  uescstr(        vTmp    );

                                 if(TOKEN_FIELD == vTokenTmp)  {
                                    dsa->fldDelimiter    =  sqlite3_mprintf( vToken );
                                    dsa->fldDelimiterLen =  strlen(          vToken );
                                 }
                                 if(TOKEN_RECORD== vTokenTmp)  {
                                    dsa->recDelimiter    =  sqlite3_mprintf( vToken );
                                    dsa->recDelimiterLen =  strlen(          vToken );
                                 }

                                 free( vToken );
                                 free( vTmp   );
                                 break;
                           case  TOKEN_SPACE:
                                 if(TOKEN_FIELD == vTokenTmp)  break;
                           default:
                                 rc = SQLITE_ERROR;
                                 fprintf( stderr ,"TFRERR: Expecting a quoted %s delimiter value.\n" ,(TOKEN_FIELD == vTokenTmp ? "FIELD" : "RECORD"));
                                 break;
                        }
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting TERMINATED keyword after %s.\n" ,(TOKEN_FIELD == vTokenTmp ? "FIELD" : "RECORD"));
                        break;
               }
#if 0
            }else{
               rc = SQLITE_ERROR;
               fprintf( stderr ,"TFRERR: Encountered illegal out of header context word %s.\n" ,(TOKEN_FIELD == vTokenID ? "FIELD" : "RECORD"));
               break;
            }
#endif
               break;
         case  TOKEN_SKIP:
               vSyntxSeg=  vTokenID;
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);

               switch ( vTokenID )
               {
                  case  TOKEN_INTEGER:
                        vToken         =  substr(  vzStr ,vTokenPos ,vTokenLen );
                        dsa->recsToSkip=  atoi( vToken );
                        free( vToken );

                        vPos     =  0;
                        vTokenID =  tfrGetTokenID( vzStr+(vTokenPos+vTokenLen) ,&vPos ,&vLen );  // Look ahead one token.

                        dsa->typeToSkip= dsa->typeToSkip | TFR_SKIP_RECORD_DATASET;

                        switch ( vTokenID )
                        {
                           case  TOKEN_PERFILE:
//                               dsa->typeToSkip= dsa->typeToSkip |(TFR_SKIPTYPE_PERFILE << TFR_SKIPTYPE_RECORD);
                                 dsa->typeToSkip= dsa->typeToSkip | TFR_SKIP_RECORD_PERFILE;
                           case  TOKEN_DATASET:
//                         default:
                                 vzStr   +=  vTokenPos+vTokenLen; // Look ahead is valid therefore advance it.
                                 vTokenPos = vPos;
                                 vTokenLen = vLen;
//                         case  TOKEN_BUFFERSIZE:
//                         case  TOKEN_ENDOFSTRING:
//                               break;
                           default:
//                               rc = SQLITE_ERROR;
//                               fprintf( stderr ,"TFRERR: Expecting DATASET or PERFILE keyword for SKIP directive.\n");
                                 break;
                        }
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting an integer value of records to skip.\n");
                        break;
               }
               break;
         case  TOKEN_ORDERED:
               vSyntxSeg=  vTokenID;
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);

               switch ( vTokenID )
               {
                  case  TOKEN_BY:
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
                  case  TOKEN_DATASET:
                  case  TOKEN_PERFILE:
                  default:
                        break;
               }
               switch ( vTokenID )
               {
                  case  TOKEN_PERFILE:
                        dsa->orderedBy = TFR_ORDEREDBY_PERFILE;
                        break;
                  case  TOKEN_DATASET:
                  default:
                        dsa->orderedBy = TFR_ORDEREDBY_DATASET;
//                default:
//                      rc = SQLITE_ERROR;
//                      fprintf( stderr ,"TFRERR: Expecting either DATASET or PERFILE keyword for ORDERED directive.\n");
                        break;
               }
               break;
         case  TOKEN_CHARSET:
               vSyntxSeg=  vTokenID;
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);

               switch ( vTokenID )
               {
                  case  TOKEN_ASCII:
                        dsa->characterSet =  TFR_CHARSET_ASCII;
                        break;
                  case  TOKEN_EBCDIC:
                        dsa->characterSet =  TFR_CHARSET_EBCDIC;
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting either ASCII or EBCDIC for character set.\n");
                        break;
               }
               break;
         case  TOKEN_ENCODING:
               vSyntxSeg=  vTokenID;
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
               switch ( vTokenID )
               {
                  case  TOKEN_UTF8:
                        dsa->textEncoding =  TFR_ENCODING_UTF8 ;
                        break;
                  case  TOKEN_UTF16:
                  case  TOKEN_UTF16BE:
                  case  TOKEN_UTF16LE:
                        dsa->textEncoding =  TFR_ENCODING_UTF16;
                        break;
                  case  TOKEN_UTF32:
                  case  TOKEN_UTF32BE:
                  case  TOKEN_UTF32LE:
                        dsa->textEncoding =  TFR_ENCODING_UTF32;
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting either UTF8 ,UTF16 or UTF32 for character encoding.\n");
                        break;
               }
               break;
         case  TOKEN_BYTEORDER:
               vSyntxSeg=  vTokenID;
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
               switch ( vTokenID )
               {
                  case  TOKEN_BIG:
                        dsa->byteOrder =  TFR_BYTEORDER_BIG;
                        break;
                  case  TOKEN_LITTLE:
                        dsa->byteOrder =  TFR_BYTEORDER_LITTLE;
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting either BIG or LITTLE endian for byte order.\n");
                        break;
               }
               break;
         case  TOKEN_BUFFERSIZE:
               vSyntxSeg=  vTokenID;
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
               switch ( vTokenID )
               {
                  case  TOKEN_INTEGER:
                        vToken   = substr( vzStr ,vTokenPos ,vTokenLen );
                        int iVal = atoi( vToken );
                        free( vToken );

                        vzStr   +=  vTokenPos+vTokenLen;    // Advance the pointer to the new starting address.
                        vTokenPos=  0;
                        vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
                        switch ( vTokenID )
                        {
                           case  TOKEN_KiB:
                                 dsa->bufferSize = iVal*1024;
                                 break;
                           case  TOKEN_MiB:
                                 dsa->bufferSize = iVal*1024*1024;
                                 break;
                           case  TOKEN_GiB:
                                 dsa->bufferSize = iVal*1024*1024*1024;
                                 break;
                           default:
                                 rc = SQLITE_ERROR;
                                 fprintf( stderr ,"TFRERR: Expecting either K(ilo) ,M(ega) ,G(iga) unit of measure for buffer size.\n");
                                 break;
                        }
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting an integer value for buffer size.\n");
                        break;
               }
               break;
         default:
               fprintf( stderr ,"%s\n"     ,pzStr   );
               fprintf( stderr ,"%s '%s'\n",TFR_ERRUNRECONLITERAL ,vToken);

               vTokenPos=  0;
               vTokenLen=  0;
               rc       =  SQLITE_ERROR;
               break;
      }
      if(NULL != vToken){ free( vToken ); vToken = NULL; }

      // Advance the pointer to the start of the next thing to tokenize out.
      #if 0
      printf("PREV TokenID= %d\tPos= %d\tLen= %d\t%p\t%s\n" ,vTokenID   , vTokenPos , vTokenLen ,&vzStr ,substr(vzStr ,vTokenPos ,vTokenLen) );
      #endif
      vzStr   +=  vTokenPos+vTokenLen; // Advance the pointer to the new starting address.
      vTokenPos=  0;

      // Get the next token.
      vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen );
      vToken   =  substr(        vzStr , vTokenPos , vTokenLen );
      #if 0
      printf("NEXT TokenID= %d\tPos= %d\tLen= %d\t%p\t%s\n" ,vTokenID   , vTokenPos , vTokenLen ,&vzStr ,substr(vzStr ,vTokenPos ,vTokenLen) );
      #endif
      vzStr   +=  vTokenPos+vTokenLen; // Ready the pointer at the start of the next thing to tokenize out since the we already know the current KEYWORD (Our grammar starts with KEYWORD).

   }while( TOKEN_ENDOFSTRING != vTokenID  && SQLITE_OK == rc );

   if(NULL  == dsa->recDelimiter)
   {
      dsa->recDelimiter    =  sqlite3_mprintf("\n");
      dsa->recDelimiterLen =  1;
   }

   return   rc;
}


// Populate the field attribute structure.
//
// TODO: See tfrInitDSA().
//
static int
tfrInitField( FieldAttr *fieldAttr ,char *pzStr ,char *pzErrMsg )
{
            int   i,rc        =  SQLITE_OK;
            char *vzStr       =  pzStr;
            char *vToken      =  NULL;
            char *vTmp        =  NULL;
   unsigned char  vTokenID    =  TOKEN_NONE;
   unsigned char  vSyntxSeg   =  TOKEN_NONE; // Current syntax segment
   unsigned short vTokenLen   =  0;
   unsigned short vTokenPos   =  0;
   unsigned short vErrorPos   =  0;
   unsigned short vPrevPos    =  0;

   Mapping        mapping;

   // Set the default.
   fieldAttr->dataType  =  SQLITE_TEXT;
   fieldAttr->visibility=  TFR_VISIBILITY_VISIBLE;

   do{
      switch ( vTokenID )
      {
         case  TOKEN_NONE:
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
               switch ( vTokenID )
               {
                  case  TOKEN_LITERAL:
                        vToken               =  substr( vzStr  ,vTokenPos  ,vTokenLen  );
                        fieldAttr->fieldName =  sqlite3_mprintf(vToken  );
                        free( vToken );
                        break;
                  case  TOKEN_STRING:
                        vToken               =  substr( vzStr  ,vTokenPos+1,vTokenLen-2);
                        fieldAttr->fieldName =  sqlite3_mprintf(vToken  );
                        free( vToken );
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting a literal for field name.\n");
                        break;
               }
               break;
         case  TOKEN_TEXT:
         case  TOKEN_VARCHAR:
         case  TOKEN_INT:
         case  TOKEN_FLOAT:
         case  TOKEN_BLOB:
               switch ( vTokenID )
               {
                  case  TOKEN_TEXT:
                  case  TOKEN_VARCHAR:
                        fieldAttr->dataType  =  SQLITE_TEXT;
                        break;
                  case  TOKEN_INT:
                        fieldAttr->dataType  =  SQLITE_INTEGER;
                        break;
                  case  TOKEN_FLOAT:
                        fieldAttr->dataType  =  SQLITE_FLOAT;
                        break;
                  case  TOKEN_BLOB:
                        fieldAttr->dataType  =  SQLITE_BLOB;

                        // Look ahead one token for a string that can be the either HEX or OCTAL..
                        unsigned short  vLen =  0;
                        unsigned short  vPos =  0;
                        vTokenID = tfrGetTokenID( vzStr+vTokenPos+vTokenLen ,&vPos ,&vLen);

                        if(TOKEN_HEXADECIMAL == vTokenID || TOKEN_OCTAL == vTokenID )
                        {
                           switch ( vTokenID )
                           {
                              case  TOKEN_BINARY:
                                    fieldAttr->blobType= TFR_BLOBTYPE_BINARY;
                                    break;
                              case  TOKEN_HEXADECIMAL:
                                    fieldAttr->blobType= TFR_BLOBTYPE_HEX;
                                    break;
                              case  TOKEN_OCTAL:
                                    fieldAttr->blobType= TFR_BLOBTYPE_OCTAL;
                              default:
                                    break;
                           }
                           vTokenPos += vPos;
                           vTokenLen  = vLen;
                        }
                  default:
                        break;
               }
               vTokenPos = 0;
               vTokenLen = 0;
               break;
         case  TOKEN_OPTIONALLY:
               break;
         case  TOKEN_ENCLOSED:
               vSyntxSeg=  vTokenID;
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);

               switch ( vTokenID )
               {
                  case  TOKEN_BY:
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenPos=  0;
                        vTokenLen=  0;
                        vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
                  default:
                        break;
               }
               switch ( vTokenID )
               {
                  case  TOKEN_STRING:
                        vToken   =  substr( vzStr ,vTokenPos+1 ,vTokenLen-2 );
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenPos=  0;
                        vTokenLen=  0;
                        fieldAttr-> leftEnclose    =  sqlite3_mprintf( vToken );
                        fieldAttr-> leftEncloseLen =  strlen(          vToken );
                        free( vToken );
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting a quoted string for the left enclosure.\n");
                        break;
               }

               // Look ahead one token for a string.
               unsigned short  vLen =  0;
               unsigned short  vPos =  0;
               vTokenID =  tfrGetTokenID( vzStr ,&vPos ,&vLen );

               char     needAND  =  0;
               switch ( vTokenID )
               {
                  case  TOKEN_AND:
                        vzStr   +=  vPos+vLen;
                        vTokenPos=  0; vPos= 0;
                        vTokenLen=  0; vLen= 0;
                        vTokenID =  tfrGetTokenID( vzStr ,&vPos ,&vLen );
                        needAND  =  1;
                  default:
                        break;
               }
               switch ( vTokenID )
               {
                  case  TOKEN_STRING:
                        vToken   =  substr( vzStr ,vPos+1 ,vLen-2 );
                        vzStr   += (vPos+vLen);
                        vTokenPos=  0;
                        vTokenLen=  0;
                        fieldAttr-> rightEnclose   =  sqlite3_mprintf( vToken );
                        fieldAttr-> rightEncloseLen=  strlen(          vToken );
                        free( vToken );
                        break;
                  default:
                        if(!needAND){
                           if(NULL  != fieldAttr-> leftEnclose){
                              fieldAttr-> rightEnclose   =  sqlite3_mprintf( fieldAttr-> leftEnclose );
                              fieldAttr-> rightEncloseLen=                   fieldAttr-> leftEncloseLen;
                           }
                        }else{
                           rc = SQLITE_ERROR;
                           fprintf( stderr ,"TFRERR: Expecting a quoted string for the right enclosure.\n");
                        }
                        break;
               }
               break;
         case  TOKEN_DELIMITER:
               vTokenID = tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);

               switch ( vTokenID )
               {
                  case  TOKEN_BY:
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenPos=  0;
                        vTokenLen=  0;
                        vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
                  default:
                        break;
               }
               switch ( vTokenID )
               {
                  case  TOKEN_STRING:
                        vTmp                    =  substr( vzStr  ,vTokenPos+1,vTokenLen-2);
                        vToken                  =  uescstr(        vTmp    );
                        fieldAttr->delimiter    =  sqlite3_mprintf(vToken  );
                        fieldAttr->delimiterLen =  strlen(         vToken  ); // vTokenLen;
                        free( vToken );
                        free( vTmp   );
                  case  TOKEN_SPACE:
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting a quoted field terminator value.\n");
                        break;
               }
               break;
         case  TOKEN_SUBDELIMITER:
               vTokenID = tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
               switch ( vTokenID )
               {
                  case  TOKEN_STRING:
                        vTmp                       =  substr( vzStr  ,vTokenPos+1,vTokenLen-2);
                        vToken                     =  uescstr(        vTmp    );
                        fieldAttr->subDelimiter    =  sqlite3_mprintf(vToken  );
                        fieldAttr->subDelimiterLen =  strlen(         vToken  ); // vTokenLen;
                        free( vToken );
                        free( vTmp   );
                  case  TOKEN_SPACE:
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting a quoted field sub-delimiter value.\n");
                        break;
               }
               break;
         case  TOKEN_POSITION:
               rc = SQLITE_ERROR;

               // I prefer if/else over switch to parse the syntax segment.
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
               if(TOKEN_LPARENTHESES== tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen)){
                  vzStr   +=  vTokenPos+vTokenLen;
                  vTokenPos=  0;

                  if(TOKEN_INTEGER  == tfrGetTokenID(   vzStr ,&vTokenPos ,&vTokenLen)){
                     vToken                  =  substr( vzStr ,vTokenPos ,vTokenLen );
                     fieldAttr->fromPosition =  atoi(   vToken );
                     free( vToken );

                     vzStr   +=  vTokenPos+vTokenLen;
                     vTokenPos=  0;

                     if(TOKEN_DASH == tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen)){
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenPos=  0;

                        if(TOKEN_INTEGER  == tfrGetTokenID(   vzStr ,&vTokenPos ,&vTokenLen)){
                           vToken                  =  substr( vzStr ,vTokenPos ,vTokenLen );
                           fieldAttr->thruPosition =  atoi(   vToken );
                           free( vToken );

                           vzStr   +=  vTokenPos+vTokenLen;
                           vTokenPos=  0;

                           if(TOKEN_RPARENTHESES== tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen)){
                              rc = SQLITE_OK;   // Finally the entire POSITION sequence is complete.
                           }
                           else
                              fprintf( stderr ,"TFRERR: Expecting a right parentheses.\n");
                        }
                        else
                           fprintf( stderr ,"TFRERR: Expecting an integer for ending position.\n");
                     }
                     else
                        fprintf( stderr ,"TFRERR: Expecting a colon/dash between integer position.\n");
                  }
                  else
                     fprintf( stderr ,"TFRERR: Expecting an integer for starting position.\n");
               }
               else
                  fprintf( stderr ,"TFRERR: Expecting a left parentheses.\n");
               break;

         case  TOKEN_ESCAPE:
               vTokenID = tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
               switch ( vTokenID )
               {
                  case  TOKEN_WITH:
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenPos=  0;
                        vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
                  default:
                        break;
               }
               switch ( vTokenID )
               {
                  case  TOKEN_STRING:
                        vToken   =  substr( vzStr ,vTokenPos+1 ,vTokenLen-2 );
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenPos=  0;
                        vTokenLen=  0;
                        fieldAttr-> escString   =  sqlite3_mprintf( vToken );
                        fieldAttr-> escStringLen=  strlen(          vToken );
                        free( vToken );
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting a quoted string for escape string.\n");
                        break;
               }
               break;
         case  TOKEN_ENCODING:
               vSyntxSeg=  vTokenID;
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
               switch ( vTokenID )
               {
                  case  TOKEN_PERCENT:
                        fieldAttr->charEncoding =  TFR_ENCODING_PERCENT;
                        break;
                  case  TOKEN_ENTITY:
                        fieldAttr->charEncoding =  TFR_ENCODING_ENTITY;
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting either PERCENT or ENTITY for text encoding.\n");
                        break;
               }
               break;
         case  TOKEN_ORDER:
               vTokenID = tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
               switch ( vTokenID )
               {
                  case  TOKEN_INTEGER:
                        vToken                  = substr( vzStr ,vTokenPos ,vTokenLen );
                        fieldAttr->orderPosition= atoi( vToken );
                        free( vToken );
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting a integer order value.\n");
                        break;
               }
               break;
         case  TOKEN_CONVERT:
               fieldAttr->mapList=sqlite3_realloc( fieldAttr->mapList ,sizeof(Mapping)*(fieldAttr->mapCount+1) );

               memset(&(fieldAttr->mapList[fieldAttr->mapCount]) ,'\0',sizeof(Mapping));  // Initialize the structure to NULL.
               vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);

               switch ( vTokenID )
               {
                  case  TOKEN_FROM:
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenPos=  0;
                        vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
                  case  TOKEN_NULL:
                  case  TOKEN_STRING:
                  default:
                        break;
               }
               switch ( vTokenID )
               {
                  case  TOKEN_STRING:
                        vToken   =  substr( vzStr  ,vTokenPos+1,vTokenLen-2);

                        fieldAttr-> mapList[fieldAttr->mapCount].mapFrom   =sqlite3_mprintf(vToken );
                        fieldAttr-> mapList[fieldAttr->mapCount].mapFromLen=        strlen( vToken );
                        free( vToken );
                  case  TOKEN_NULL:
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenPos=  0;
                        vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting a string value or NULL literal to convert FROM.\n");
                        break;
               }
               switch ( vTokenID )
               {
                  case  TOKEN_TO:
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenPos=  0;
                        vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
                  case  TOKEN_NULL:
                  case  TOKEN_STRING:
                  default:
                        break;
               }
               switch ( vTokenID )
               {
                  case  TOKEN_STRING:
                        vToken   =  substr( vzStr  ,vTokenPos+1,vTokenLen-2);

                        fieldAttr-> mapList[fieldAttr->mapCount].mapTo     =sqlite3_mprintf(vToken );
                        fieldAttr-> mapList[fieldAttr->mapCount].mapToLen  =        strlen( vToken );
                        free( vToken );
                  case  TOKEN_NULL:
                        vzStr   +=  vTokenPos+vTokenLen;
                        vTokenPos=  0;
                        vTokenLen=  0;
//                      vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen);
                        // Finally this syntax segment is complete!
                        fieldAttr-> mapCount++;
                        break;
                  default:
                        rc = SQLITE_ERROR;
                        fprintf( stderr ,"TFRERR: Expecting a string value or NULL literal to convert TO.\n");
                        break;
               }
//             fieldAttr->mapCount++;

               break;
         case  TOKEN_HIDDEN:
         case  TOKEN_IGNORE:
               switch ( vTokenID )
               {
                  case  TOKEN_HIDDEN:
                        fieldAttr->visibility=  TFR_VISIBILITY_HIDDEN;
                        break;
                  case  TOKEN_IGNORE:
                        fieldAttr->visibility=  TFR_VISIBILITY_IGNORE;
                  default:
                        break;
               }
               vTokenPos = 0;
               vTokenLen = 0;
               break;
         default:
               fprintf( stderr ,"%s\n" ,pzStr   );
               fprintf( stderr ,"%s\n" ,TFR_ERRUNRECONLITERAL   );

               vTokenPos = 0;
               vTokenLen = 0;
               rc        = SQLITE_ERROR;
               break;
      }
      if( vToken != NULL ) { free( vToken ); vToken = NULL; }

      // Advance the pointer to the start of the next thing to tokenize out.
      #if 0
      printf("PREV TokenID= %d\tPos= %d\tLen= %d\t%p\t%s\n" ,vTokenID   , vTokenPos , vTokenLen ,&vzStr ,substr(vzStr ,vTokenPos ,vTokenLen) );
      #endif
      vzStr   +=  vTokenPos+vTokenLen; // Advance the pointer to the new starting address.
      vTokenPos=  0;

      // Get the next token.
      vTokenID =  tfrGetTokenID( vzStr ,&vTokenPos ,&vTokenLen );
      vToken   =  substr(        vzStr , vTokenPos , vTokenLen );
      #if 0
      putchar('\n');
      printf("NEXT TokenID= %d\tPos= %d\tLen= %d\t%p\t%s\n" ,vTokenID   , vTokenPos , vTokenLen ,&vzStr ,substr(vzStr ,vTokenPos ,vTokenLen) );
      #endif
      vzStr   +=  vTokenPos+vTokenLen; // Ready the pointer at the start of the next thing to tokenize out since the we already know the current KEYWORD (Our grammar starts with KEYWORD).

   }while( TOKEN_ENDOFSTRING != vTokenID  && SQLITE_OK == rc );

   return   rc;
}

// End of parser section.

static
int   setFileAttribute( FileAttr *pFileAttr ,const  char *pFullPathName ,char *pzErrMsg )
{
   int      i,rc = EXIT_SUCCESS;
   int      strLen   = 0;
   int      strMrk   = 0;
   char     modifiedOn[32];
   char    *tmpValue =  NULL;
   char     strValue[   PATH_MAX   ];
   struct   stat  fileStatus;

   rc = stat( pFullPathName  ,&fileStatus );

   if(EXIT_SUCCESS != rc)  pzErrMsg =  sqlite3_mprintf("TFRERR: File stat: %s\n\t%s\n" ,pFullPathName ,strerror( errno ));
   else{
      memset( pFileAttr ,'\0' ,sizeof(FileAttr) );     // Initialize the structure to NULL.

      pFileAttr->fullName     =  sqlite3_mprintf("%s" ,pFullPathName  );
      pFileAttr->fileType     =  TFR_FILETYPE_TEXT;// TODO: Figure the type of file.
      pFileAttr->fileSize     =  fileStatus.st_size;
      pFileAttr->statMode     =  fileStatus.st_mode;

      strcpy(    strValue       ,pFullPathName  );
      tmpValue = basename(       strValue       ); // Don't switch the order!
      pFileAttr->baseName     =  sqlite3_mprintf("%s" ,tmpValue   );
      pFileAttr->baseNameLen  =  strlen( pFileAttr->baseName      );
      free(      tmpValue );
      tmpValue = dirname(        strValue       ); // Don't switch the order!
      pFileAttr->dirName      =  sqlite3_mprintf("%s" ,tmpValue);
      pFileAttr->dirNameLen   =  strlen( pFileAttr->dirName    );
      free(      tmpValue );

      strftime(  modifiedOn     ,24 ,"%Y-%m-%d %a %H:%M:%S" ,localtime(&(fileStatus.st_mtime))); // 2013-12-15 Sun 21:53:08
      pFileAttr->modifiedOn   =  sqlite3_mprintf("%s"       ,modifiedOn );
      pFileAttr->modifiedOnLen=  strlen( pFileAttr->modifiedOn );
      pFileAttr->next         =  NULL;

      // TODO: Investigate why "baseName=sqlite3_mprintf() or basename()" indirectly corrupts the memory of TfrVTab->sqlite3_vtab->vtzErrMsg !!!
      //       It looks like writing into baseName screw things up!!!

      // TODO: Test if the file can be open.  Not foolproof but a nice check.
      //       open() with O_DIRECT and O_LARGEFILE and O_NOATIME
      //       Check out posix_fadvise()
      //       Checkout aio.h
      //       Chckout  mman.h
   }

   return   rc;
}

// Pass the FileAttr back constructed from the fullpathname.
//
static
FileAttr *newFileAttribute( char *pFullpathname ,char *pzErrMsg )
{
   int        rc       = EXIT_SUCCESS;
   FileAttr  *fileAttr = NULL;

   fileAttr = sqlite3_malloc( sizeof(FileAttr));

   if(NULL == fileAttr) pzErrMsg =  sqlite3_mprintf("TFRERR: Allocating memory for FileAttr struct!");
   else{
      rc = setFileAttribute( fileAttr ,pFullpathname ,pzErrMsg );

      if(EXIT_SUCCESS != rc)
      {
         sqlite3_free( fileAttr );
         fileAttr = NULL;
      }
   }

   return   fileAttr;
}

static
int   appendGlobListToCursor( CursorCtrl *pCursorCtrl ,glob_t *globBuf ,char *pzErrMsg )
{
   int   i,j;
   int   rc =  EXIT_SUCCESS;

   for( i = 0 ; i < globBuf->gl_pathc && EXIT_SUCCESS == rc ; i++ )
   {
      FileAttr *fileAttr;
      fileAttr=(FileAttr *)newFileAttribute( globBuf->gl_pathv[i] ,pzErrMsg );

      if(NULL != fileAttr )
      {
         #if 0
         printf("%p\tmode=%d\tisReg=%d isDir=%d isBlk=%d isChr=%d isFifo=%d\n",fileAttr,(*fileAttr).statMode
                                                                              ,S_ISREG( (*fileAttr).statMode)   // Test for a regular file.
                                                                              ,S_ISDIR( (*fileAttr).statMode)   // Test for a directory.
                                                                              ,S_ISBLK( (*fileAttr).statMode)   // Test for a block special file.
                                                                              ,S_ISCHR( (*fileAttr).statMode)   // Test for a character special file.
                                                                              ,S_ISFIFO((*fileAttr).statMode)   // Test for a pipe or FIFO special file.
         );
         #endif
         if(S_ISREG((*fileAttr).statMode ))
         {
            if(NULL==(pCursorCtrl->fileList))    // No entries for the file list yet.
                      pCursorCtrl->fileList      =  fileAttr;
            else      pCursorCtrl->currFile->next=  fileAttr;

            pCursorCtrl->fileCount++;
            pCursorCtrl->currFile=  fileAttr;
         }
         else if(S_ISDIR((*fileAttr).statMode ))
         {
            char  *sDirName = sqlite3_mprintf("%s/*"  ,globBuf->gl_pathv[i] );

            if(NULL != sDirName )
            {
               glob_t   globBuf2;
               memset( &globBuf2 ,'\0' ,sizeof(glob_t) );   // Initialize the structure to NULL.

               rc = glob( sDirName ,(GLOB_MARK | GLOB_NOESCAPE) ,NULL ,&globBuf2 );
               if(GLOB_SUCCESS == rc && globBuf2.gl_pathc > 0 )
               {
                  pCursorCtrl->sdirCount++;

                  rc =  appendGlobListToCursor( pCursorCtrl ,&globBuf2 ,pzErrMsg );
               }

               globfree( &globBuf2 );
            }

            sqlite3_free( sDirName );
         }
      }else rc = EXIT_FAILURE;   // To break out of the loop.  Defined in stdlib.h
   }

   return   rc;
}


// Free the Dataset Attribute struct off its allocated memory.
//
static
void  freeDataSetAttr( DatasetAttr *pDsa )
{
   DEBUG_APITRACE("In    freeDataSetAttr() ...\n");
   register int   i,j;

   // Free the field array.
   for( i = 0 ; i < pDsa->fieldCount ; i++ )
   {
      for( j = 0 ; j < pDsa->fieldList[i].mapCount ; j++ )
      {
         sqlite3_free( pDsa->fieldList[i].mapList[j].mapFrom);
         sqlite3_free( pDsa->fieldList[i].mapList[j].mapTo  );
      }
      sqlite3_free( pDsa->fieldList[i].mapList     );   // All fields was allocated as an array.

      sqlite3_free( pDsa->fieldList[i].fieldName   );
      sqlite3_free( pDsa->fieldList[i].delimiter   );
      sqlite3_free( pDsa->fieldList[i].subDelimiter);
      sqlite3_free( pDsa->fieldList[i].leftEnclose );
      sqlite3_free( pDsa->fieldList[i].rightEnclose);
   }
   sqlite3_free( pDsa->fieldList    );   // All fields was allocated as an array.
   sqlite3_free( pDsa->fldDelimiter );
   sqlite3_free( pDsa->recDelimiter );
   sqlite3_free( pDsa->fileGlob     );
   sqlite3_free( pDsa );

   pDsa = NULL;

   DEBUG_APITRACE("Done  freeDataSetAttr() ...\n");
}

// The original approach was to allocate the memory in this routine and return the pointer.  This caused the base.zErrMsg to be corrupted and crash.
static
DatasetAttr *setDataSetAttr( DatasetAttr *dsa ,const int pArgc ,const char * const *pArgv ,char *pzErrMsg )
{
   DEBUG_APITRACE("In    setDataSetAttr() ...\n");
   int   i,j,rc=  SQLITE_OK;
   pzErrMsg    =  NULL;

   memset( dsa ,'\0' ,sizeof(DatasetAttr));  // Initialize the structure to NULL.

   char *vArg  = sqlite3_mprintf( pArgv[3]); // Table parameters.
   rc =  tfrInitDSA( dsa ,vArg ,pzErrMsg  );
   sqlite3_free(          vArg            );

   if(SQLITE_OK == rc)
   {
      // Allocate Field Attributes.
      dsa->fieldCount=  pArgc -4;
      dsa->fieldList =  sqlite3_malloc(sizeof(FieldAttr) * dsa->fieldCount);
      if(NULL == dsa->fieldList )
         pzErrMsg =  sqlite3_mprintf("TFRERR: Allocating memory for %d FieldAttr struct!" ,dsa->fieldCount );
      else{
         rc = SQLITE_OK;
         memset( dsa->fieldList ,'\0' ,sizeof(FieldAttr) * dsa->fieldCount);   // Initialize the structure to NULL.

         // Alias the path to the field array.
         FieldAttr *fields = dsa->fieldList;

         for( i=0 ; i < (pArgc-4) ; i++ )
         {
            vArg  = sqlite3_mprintf("%s"       ,pArgv[4+i]     ); // Column parameters.
            rc    = tfrInitField(&(fields[ i]) ,vArg ,pzErrMsg );
            sqlite3_free(                       vArg           );

            if(0< i
            && 0< fields[  0].thruPosition // First field started with position spec.
            &&    fields[i-1].thruPosition >= fields[i].fromPosition
            ){
               rc = SQLITE_ERROR;
               fprintf( stderr ,"TFRERR: Field %s(%d,%d) overlap %s(%d,%d).\n",fields[i-1].fieldName
                                                                              ,fields[i-1].fromPosition ,fields[i-1].thruPosition
                                                                              ,fields[i  ].fieldName
                                                                              ,fields[i  ].fromPosition ,fields[i  ].thruPosition
               );
            }else{
               if((i+1) < (pArgc-4)             // Not the last field.
               && NULL  == fields[i].delimiter  // Field delimeter NOt specify.
               && NULL  != dsa->fldDelimiter    // Global field delimeter is specify.
               ){
                  fields[ i].delimiter    =  sqlite3_mprintf("%s",dsa->fldDelimiter);
                  fields[ i].delimiterLen =                       dsa->fldDelimiterLen;
               }
            }
         }

         // Default the last field if not set.
         if(NULL == fields[i-1].delimiter)   // Current value in "i" terminated the above loop.
         {
            fields[i-1].delimiter   = (NULL != dsa->recDelimiter ? dsa->recDelimiter : sqlite3_mprintf("\n"));
            fields[i-1].delimiterLen=  strlen(fields[i-1].delimiter);
         }
      }
   }

   DEBUG_APITRACE("Done  setDataSetAttr() ...\n");
   return   dsa;
}

static
void  resetCursorCtrl( CursorCtrl *pCursorCtrl )
{
   pCursorCtrl->rowNumber     =  0;
   pCursorCtrl->recNumber     =  0;
   pCursorCtrl->currFile      =  pCursorCtrl->fileList;
   pCursorCtrl->currFileHandle=  0;
   pCursorCtrl->readBufferSize=  0;
   pCursorCtrl->fileBufferPos =  0;
   pCursorCtrl->fieldStartPos =  0;
   pCursorCtrl->currField     =  0;
   pCursorCtrl->isEndOfRec    =  0;
   pCursorCtrl->isLeftEncl    =  0;
   pCursorCtrl->currRBC       =  0;
}

static
void  freeCursorCtrl( CursorCtrl *pCursorCtrl )
{
   DEBUG_APITRACE("In    freeCursorCtrl() ...\n");

   // Free the Cursor Control.
   if(NULL != pCursorCtrl->recBufferCtrl  )  sqlite3_free( pCursorCtrl->recBufferCtrl  );
   if(NULL != pCursorCtrl->recFilterCtrl  )  sqlite3_free( pCursorCtrl->recFilterCtrl  );
   if(NULL != pCursorCtrl->recBuffer      )  sqlite3_free( pCursorCtrl->recBuffer      );
   if(NULL != pCursorCtrl->fileBuffer0    )  sqlite3_free( pCursorCtrl->fileBuffer0    );
   if(NULL != pCursorCtrl->hostName       )  sqlite3_free( pCursorCtrl->hostName       );

   // Free the file list.
   {
      FileAttr *currNode = pCursorCtrl->fileList;
      FileAttr *tempNode;

      while(NULL != currNode )
      {
         tempNode = currNode;
         currNode = currNode->next;

         if(NULL != tempNode->modifiedOn  )  sqlite3_free( tempNode->modifiedOn  );
         if(NULL != tempNode->baseName    )  sqlite3_free( tempNode->baseName    );
         if(NULL != tempNode->dirName     )  sqlite3_free( tempNode->dirName     );
         if(NULL != tempNode->fullName    )  sqlite3_free( tempNode->fullName    );

         sqlite3_free( tempNode );
      }
   }

   sqlite3_free( pCursorCtrl );
   pCursorCtrl =  NULL;

   DEBUG_APITRACE("Done  freeCursorCtrl() ...\n");
}

static
RecBufCtrl *newRecBufferCtrl( DatasetAttr *pDsa ,const unsigned short pArraySize ,const unsigned char pAllFields ,char *pzErrMsg )
{
   DEBUG_APITRACE("In    newRecBufferCtrl() ...\n");
   int         i,j=0;
   RecBufCtrl *recBufCtrl;

   // Allocated the record Buffer Control Array.
   recBufCtrl = sqlite3_malloc( sizeof(RecBufCtrl) * pArraySize );

   if(NULL == recBufCtrl )  pzErrMsg =  sqlite3_mprintf("TFRERR: Allocating memory for %d RecBufCtrl struct!" ,pArraySize );
   else{
      memset( recBufCtrl ,'\0' ,sizeof(RecBufCtrl) * pArraySize );

      // Attach the field to the Record Buffer Control entries.
      for( i = 0 ; i < pDsa->fieldCount ; i++ )
      {
         if(TFR_VISIBILITY_IGNORE !=  pDsa->fieldList[i].visibility  || pAllFields  )
         {
            recBufCtrl[j++].field = &(pDsa->fieldList[i]);
         }
      }
   }

   DEBUG_APITRACE("Done  newRecBufferCtrl() ...\n");
   return   recBufCtrl;
}

static
CursorCtrl  *newCursorCtrl(   DatasetAttr *pDsa ,char *pzErrMsg )
{
   DEBUG_APITRACE("In    newCursorCtrl() ...\n");
   int         i, j, rc    =  SQLITE_NOMEM;

   pzErrMsg =  NULL;
   CursorCtrl *pCursorCtrl =  sqlite3_malloc(sizeof(CursorCtrl));

   if(NULL == pCursorCtrl ) pzErrMsg =  sqlite3_mprintf("TFRERR: Allocating memory for CursorCtrl struct!" );
   else{
      memset( pCursorCtrl ,'\0' ,sizeof(CursorCtrl));   // Initialize the structure to NULL.

      #if   defined(_WIN32) || defined(_WIN64)
      char *hostName = getenv("COMPUTERNAME");

      pCursorCtrl->hostName   =  sqlite3_mprintf("%s",hostName );
      pCursorCtrl->hostNameLen=  strlen( pCursorCtrl->hostName );
//    free( hostName );    // TODO: Figure out why this free() crashes!
      #endif

      pCursorCtrl->fileBuffer0 = sqlite3_malloc( TFR_FILEBUFSIZE+4 );   // Ready for SIMD 4 bytes check beyond specified  the buffer size.
      if(NULL == pCursorCtrl->fileBuffer0 )  pzErrMsg =  sqlite3_mprintf("TFRERR: Allocating memory for file buffer of size %d!" ,TFR_FILEBUFSIZE);
      else{
         pCursorCtrl->recBuffer = sqlite3_malloc( TFR_RECBUFSIZE );
         if(NULL == pCursorCtrl->recBuffer ) pzErrMsg =  sqlite3_mprintf("TFRERR: Allocating memory for record buffer of size %d!" ,TFR_RECBUFSIZE );
         else{
            pCursorCtrl->recBufferLen = TFR_RECBUFSIZE;

            memset( pCursorCtrl->recBuffer ,'\0' ,TFR_RECBUFSIZE );   // Initialize the structure to NULL.

            // Glob the file list to scan.
            glob_t   globBuf;
            memset( &globBuf  ,'\0' ,sizeof(glob_t) );
            rc = glob( pDsa->fileGlob ,(GLOB_MARK | GLOB_NOESCAPE) ,NULL ,&globBuf );

            if(GLOB_SUCCESS == rc )
            {
               rc =  appendGlobListToCursor( pCursorCtrl ,&globBuf ,pzErrMsg );
               #if 0
               displayFiles( pCursorCtrl->fileList );
               #endif

               if(EXIT_SUCCESS != rc )  pzErrMsg =  sqlite3_mprintf("TFRERR: Appending globbed files to list!" );
               else{
                  rc =  SQLITE_NOMEM;

                  // Set the current file to be the first file in the list.
                  pCursorCtrl->currFile = pCursorCtrl->fileList;

                  // Count the Record Buffer Control entries.
                  for( i = 0 ; i < pDsa->fieldCount ; i++ )
                  {
                     if(TFR_VISIBILITY_IGNORE != pDsa->fieldList[i].visibility ) pCursorCtrl->rbcCount++;
                  }

                  // Allocated an extra record to skip additional bondary check in parseOutRecord()::rbc[ cursorCtrl->currRBC+1].startPOs = ...
                  pCursorCtrl->recBufferCtrl = newRecBufferCtrl( pDsa ,pCursorCtrl->rbcCount+1 ,0 ,pzErrMsg );
               }
            }else{
               switch( rc )
               {
                  case GLOB_ABORTED :  pzErrMsg =  sqlite3_mprintf("TFRERR: glob aborted!" ); break;
                  case GLOB_NOMATCH :  pzErrMsg =  sqlite3_mprintf("TFRERR: glob noMatch!" ); break;
                  case GLOB_NOSPACE :  pzErrMsg =  sqlite3_mprintf("TFRERR: glob noSpace!" ); break;
               }
            }
         }
      }
   }

   DEBUG_APITRACE("Done  newCursorCtrl() ...\n");
   return   pCursorCtrl;
}

/*
Desc: This is the heart of this module.
      Base on the virtual table creation metadata, parse out the data into the record buffer in a single pass.
      The following are the type of parsing to be performed:
         1) Delimited.
               Find an equal length delimiter string pattern in the input buffer.
         2) Positional.
               Dumbly substring each field according to the start position by a given length.
         3) Whitespace.
               Find either tab or space as the delimiter.  This is shall be a variable length delimeter. A delimeter is found when current byte is a white apce and the next byte is NOT.

         Until a delimiter is triggered, copy byte-by-byte from the input buffer over to the record buffer ... unless the field is tag as to be ignored
         or it is within the boundary of a field starting and ending position.
*/
static
unsigned
short parseOutRecord( sqlite3_vtab_cursor *pVTabCur )
{
   DEBUG_APITRACE("In    parseOutRecord() ...\n");
   register int   i,j   =  0;
   register
   unsigned char  isEOR =  0; // End of Record.
   register
   unsigned char  isEOF =  0; // End of Field.
   register
   unsigned char  isEOLT=  0; // End of LEFT  enclosure.
   register
   unsigned char  isEORT=  0; // End of RIGHT enclosure.
   //
   register
   unsigned char  fldDelLen;  // Field delimiter length.
   register
   unsigned char  fldDelPos;  // Field current position in.
   register
   unsigned char  recDelLen;  // Record delimiter length.
   register
   unsigned char  recDelPos;  // Record delimiter position.
   unsigned char  subDelLen;  // Sub-Delimiter length.
   unsigned char  subDelPos;  // Sub-Delimiter position.
   unsigned char  lftEncLen;  // Enclosure left  length.
   unsigned char  lftEncPos;  // Enclosure left  position.
   unsigned char  rgtEncLen;  // Enclosure right length.
   unsigned char  rgtEncPos;  // Enclosure right position.
   unsigned char  encDelLen=0;// Length of the enclosure to strip off the record buffer.
            char *lftEnc;     // Left  enclosure string pattern.
            char *rgtEnc;     // Right enclosure string pattern.
            char *fldDel;     // Field  delimeter.
            char *subDel;     // Field  sub-delimeter.
            char *recDel;     // Record delimeter.

   // Alias them.
   TfrCursor   *pTfrCursor = (TfrCursor *)pVTabCur;
   DatasetAttr *dsa        =((TfrTable  *)pVTabCur->pVtab)->dsa;

   FieldAttr   *currField  =  NULL;                                  // Alias the path to the current field.
   FieldAttr   *fields     =         dsa->fieldList;                 // Alias the path to the field array.
   CursorCtrl  *cursorCtrl =  pTfrCursor->cursorCtrl;                // Alias the path to the cursor control.
   RecBufCtrl  *currRBC    =  NULL;
   RecBufCtrl  *rbc        =  pTfrCursor->cursorCtrl->recBufferCtrl; // Alias the path to the record buffer control array.
   char        *fileBuffer =  pTfrCursor->cursorCtrl->fileBuffer0;   // Alias the file buffer.

   // Alias the current field delimiter control.
   currField= &fields[ cursorCtrl->currField ];
   currRBC  = &rbc[    cursorCtrl->currRBC   ];

   // TODO: Direct access these carry over variables.
   lftEnc   =  currField->leftEnclose;
   lftEncLen=  currField->leftEncloseLen;
   rgtEnc   =  currField->rightEnclose;
   rgtEncLen=  currField->rightEncloseLen;
   fldDel   =  currField->delimiter;
   fldDelLen=  currField->delimiterLen;
   subDel   =  currField->subDelimiter;
   subDelLen=  currField->subDelimiterLen;
   recDel   = fields[dsa->fieldCount -1].delimiter;     // The last field.
   recDelLen= fields[dsa->fieldCount -1].delimiterLen;  // The last field.

   // Carry over position from previous buffer.
   isEOLT   = cursorCtrl->isLeftEncl;
   lftEncPos= cursorCtrl->lftEncPos;
   rgtEncPos= cursorCtrl->rgtEncPos;
   fldDelPos= cursorCtrl->fldDelPos;
   subDelPos= cursorCtrl->subDelPos;
   recDelPos= cursorCtrl->recDelPos;

   DEBUG_APITRACE("Done  intialization ...\n");

   // Start scanning byte-by-byte until the end-of-buffer or end-of-record.
   for( i = cursorCtrl->fileBufferPos ; i < cursorCtrl->readBufferSize && !isEOR ; i++ )
   {
      // Set the "end of" booleans section.
      //
      // Left enclose MUST come before right enclose so check right enclose first.
      if( isEOLT && !isEORT)
      {
         // Check for right enclosure string.
         if(rgtEncPos < rgtEncLen && fileBuffer[i] == rgtEnc[rgtEncPos] &&!isEORT) rgtEncPos++; else rgtEncPos = 0;
            isEORT=(0 < rgtEncLen && rgtEncPos     == rgtEncLen);
         if(isEORT )    encDelLen =  rgtEncLen;
      }
      if(!isEOLT)
      {
         // Check for left  enclosure string.
         if(lftEncPos < lftEncLen && fileBuffer[i] == lftEnc[lftEncPos] &&!isEOLT) lftEncPos++; else lftEncPos = 0;
            isEOLT=(0 < lftEncLen && lftEncPos     == lftEncLen);
         if(isEOLT )    encDelLen =  lftEncLen;
      }

      // Check for record delimiter which is also the field seperator.
      if(recDelPos < recDelLen && fileBuffer[i] == recDel[recDelPos] )  recDelPos++; else recDelPos = 0;

      if(0< currField->thruPosition)   // Do positional parsing.
         isEOF=(i -cursorCtrl->fieldStartPos)== currField->thruPosition;   // Position value start from 1.
      else{
         if(!isEOLT) // Left enclose NOT found/started.
         {
            if(NULL== fldDel) // No delimiter defined.
            {  // Do whitespace parsing.
               if('\t' == fileBuffer[i]   || ' ' == fileBuffer[i])
                  fldDelPos++;
               else
                  fldDelPos=0;

               isEOF=(  ('\t' == fileBuffer[i]   || ' ' == fileBuffer[i]   )     // Current char is     a space
                     &&!('\t' == fileBuffer[i+1] || ' ' == fileBuffer[i+1] ));   //but Next char is NOT a space.
            }else{            // Scan for delimiter.
               // TODO: Escape character/string handling?
               if(fileBuffer[i] == fldDel[fldDelPos] && fldDelPos < fldDelLen )
                  fldDelPos++;
               else
                  fldDelPos=0;

               isEOF=(fldDelPos == fldDelLen);
            }
         }
         else  isEOF= 0;
      }

      if(isEORT)  // Right enclose found reset booleans.
      {
         isEOLT = 0;
         isEORT = 0;
      }

      isEOR =(recDelPos == recDelLen); // End of record delimiter.  EOF will also be set since they are the same delimiter.

      #if 0
      printf("\tEOF=%d EOR=%d\t'%04d %3d %c' fld(%3d %c %1d %1d) rec(%3d %c %1d %1d)\n"
               ,isEOF,isEOR
               ,i    ,fileBuffer[i]
               ,                  '\n' != fileBuffer[i]     ? fileBuffer[i]                            : (char)254
               ,fldDel[fldDelPos],'\n' != fldDel[fldDelPos] ? fldDel[(fldDelPos==0 ? 0 : fldDelPos-1)] : (char)254 ,(fldDelPos==0 ? 0 : fldDelPos-1) ,fldDelLen
               ,recDel[recDelPos],'\n' != recDel[recDelPos] ? recDel[ recDelPos                      ] : (char)254                    , recDelPos    ,recDelLen
      );
      #endif

      if(isEOF)   // End of Field.
      {
         if(TFR_VISIBILITY_IGNORE != currField->visibility
                        &&(      0== currField->thruPosition   // NOT positional parsing.
                           ||(  (i -cursorCtrl->fieldStartPos) >= currField->fromPosition
                              &&(i -cursorCtrl->fieldStartPos) <= currField->thruPosition   )
                           )
         ){
            // We have an extra record allocated for overflow.
            currRBC[0].length  -=  fldDelPos -1;      // Set the field length for the current field without the field delimiter.
            currRBC[1].startPos =  currRBC[0].startPos
                                  +currRBC[0].length; // Set the starting position in the record buffer for the next field.

            cursorCtrl->currRBC++;
            currRBC  = &rbc[ cursorCtrl->currRBC ];   // Alias the current RBC.
         }

         if(isEOR)   // End of record.
         {
            // Omit the CR immediately before LF.
            if(0 < i &&   0  <  recDelPos
                     && '\r' != recDel[recDelPos-1] && '\n' == recDel[recDelPos]
                     && '\r' == fileBuffer[    i-1] && '\n' == fileBuffer[    i] )   currRBC[0].length--;

            recDelPos = 0;

            cursorCtrl->recSize  =  currRBC[0].startPos + currRBC[0].length;
            cursorCtrl->rowNumber++;
            cursorCtrl->recNumber++;
            // Reset the pointer control.
            cursorCtrl->currField=-1;  // It will be incremented back to zero below for start of field.
            cursorCtrl->currRBC  = 0;
            cursorCtrl->fieldStartPos=i+1;
         }

         cursorCtrl->currField++;
         currField= &fields[ cursorCtrl->currField ]; // currField!

         // Reset the delimiter for the next field.
         lftEnc   =  currField->leftEnclose;
         lftEncLen=  currField->leftEncloseLen;
         lftEncPos=  0;
         rgtEnc   =  currField->rightEnclose;
         rgtEncLen=  currField->rightEncloseLen;
         rgtEncPos=  0;
         fldDel   =  currField->delimiter;
         fldDelLen=  currField->delimiterLen;
         fldDelPos=  0;
         subDel   =  currField->subDelimiter;
         subDelLen=  currField->subDelimiterLen;
         subDelPos=  0;
      }else{ // Still have NOT hit any of the "end of" markers.
         if(TFR_VISIBILITY_IGNORE != currField->visibility
                        &&(      0== currField->thruPosition   // NOT positional parsing.
                           ||(  (i -cursorCtrl->fieldStartPos) >= currField->fromPosition -1
                              &&(i -cursorCtrl->fieldStartPos) <= currField->thruPosition -1 )
                           )
         ){
            if((currRBC[0].length + currRBC[0].startPos +1) >= cursorCtrl->recBufferLen)
            {
               // Realloc the recBuffer by 1 KiB increment.
               cursorCtrl->recBufferLen+= 1024;
               cursorCtrl->recBuffer    =(char *)  sqlite3_realloc( cursorCtrl->recBuffer ,cursorCtrl->recBufferLen );

//             sqlite3_log(SQLITE_NOTICE ,"WARNING: Record# %d increased record buffer by 1024 bytes for field#%d\n"  ,(cursorCtrl->recNumber+1) ,cursorCtrl->currField);
               fprintf(    stderr        ,"WARNING: Record# %d increased record buffer by 1024 bytes for field#%d\n"  ,(cursorCtrl->recNumber+1) ,cursorCtrl->currField);
            }

            // Copy one byte onto the record buffer pointed by the current field in the buffer control.
            cursorCtrl->recBuffer[  currRBC[0].length + currRBC[0].startPos ]=  fileBuffer[i];
                                    currRBC[0].length++;
            cursorCtrl->recBuffer[  currRBC[0].length + currRBC[0].startPos ]= '\0'; // Advance the string terminator to the next position.  Just in case there is a bug in setting the RBC entries.

            if(0 < encDelLen)
            {
               currRBC[0].length-= encDelLen;
               encDelLen =0;  // Dont it once until the next enclose.
            }
         }
      }
   }

   cursorCtrl->isEndOfRec     = isEOR;
   cursorCtrl->isLeftEncl     = isEOLT;
   cursorCtrl->lftEncPos      = lftEncPos;
   cursorCtrl->rgtEncPos      = rgtEncPos;
   cursorCtrl->fldDelPos      = fldDelPos;
   cursorCtrl->subDelPos      = subDelPos;
   cursorCtrl->recDelPos      = recDelPos;
   cursorCtrl->fileBufferPos  = i;  // Value in "i" terminated the above loop and shall be the starting position for the next record.

   DEBUG_APITRACE("Done  parseOutRecord() ...\n");
   return   cursorCtrl->recSize;
}

static
int   getNextRecord( sqlite3_vtab_cursor *pVTabCur )
{
   DEBUG_APITRACE("In    getNextRecord() ...\n");
   int   i  ,rc   = SQLITE_OK;

   // Alias them.
   TfrTable   *tfrTable  = (TfrTable  *)pVTabCur->pVtab;
   TfrCursor  *tfrCursor = (TfrCursor *)pVTabCur;
   CursorCtrl *cursorCtrl=  tfrCursor-> cursorCtrl;

   if(NULL != cursorCtrl->currFile) // Not end of list.
   {
      // Clear the record buffer control.
      for( i=0 ; i < cursorCtrl->rbcCount ; i++ )
      {
         cursorCtrl->recBufferCtrl[i].length=0;
      }

      do{
         if( 0 == cursorCtrl->currFile->fileHandle )
         {
            #ifdef   DEBUG_MODE
            printf("  Opening file: %s %d bytes\n"   ,cursorCtrl->currFile->fullName ,cursorCtrl->currFile->fileSize );
            #endif

            cursorCtrl->fieldStartPos        =        cursorCtrl->fileBufferPos -cursorCtrl->readBufferSize;   // Start first with a cerdit from last bufffer.
            cursorCtrl->currFile->fileHandle =  open( cursorCtrl->currFile->fullName  ,O_RDONLY
                                                                                      #ifndef   _WIN32
                                                                                      |O_DIRECT |O_LARGEFILE   // Non-Windows platform support.
                                                                                      #endif
                                                                                      ,S_IREAD );
            // Reset the house keeping positions.
            cursorCtrl->fieldStartPos  =  0;
            cursorCtrl->recNumber      =  0;
            cursorCtrl->fileBufferPos  =  0;
            cursorCtrl->readBufferSize =  0;
            cursorCtrl->recSize        =  0;

            // Set the record skipping counters.
            if(0< tfrTable->dsa->hdrsToSkip
               &&(cursorCtrl->fileList    == cursorCtrl->currFile       // Very first file.
               || TFR_SKIP_HEADER_PERFILE ==(tfrTable->dsa->typeToSkip & TFR_SKIP_HEADER_PERFILE))
            )     cursorCtrl->hdrSkipped  =  tfrTable->dsa->hdrsToSkip;

            if(0< tfrTable->dsa->recsToSkip
               &&(cursorCtrl->fileList    == cursorCtrl->currFile       // Very first file.
               || TFR_SKIP_RECORD_PERFILE ==(tfrTable->dsa->typeToSkip & TFR_SKIP_RECORD_PERFILE))
            )     cursorCtrl->recSkipped  =  tfrTable->dsa->recsToSkip;
         }

         // A file handle is opened and we have read to the end of the file buffer.
         if(0 != cursorCtrl->currFile->fileHandle && cursorCtrl->fileBufferPos >= cursorCtrl->readBufferSize )
         {
            cursorCtrl->readBufferSize = read( cursorCtrl->currFile->fileHandle ,cursorCtrl->fileBuffer0 ,TFR_FILEBUFSIZE );
            cursorCtrl->fileBufferPos  = 0;
            // Following are to assist with whitespace parsing.
            cursorCtrl->fileBuffer0[cursorCtrl->readBufferSize  ] ='\t';
            cursorCtrl->fileBuffer0[cursorCtrl->readBufferSize+1] ='\0';
            #ifdef   DEBUG_MODE
            printf("  Read %7d bytes into buffer ... last recNumber=%d\n" ,cursorCtrl->readBufferSize ,cursorCtrl->recNumber );
            #endif

            if(0 >= cursorCtrl->readBufferSize ) // Read until system read zero bytes, that's the EOF. -ve readBufferSize is a special indicator to close the file.
            {
               // Close the current file.
               close( cursorCtrl->currFile->fileHandle );
               cursorCtrl->currFile->fileHandle = 0;
               cursorCtrl->readBufferSize       = 0;
               cursorCtrl->isEndOfRec           = 0;  // File is closed implies end of record.  Needed to iterate one more time to open a new file.
               #ifdef   DEBUG_MODE
               printf("  Closed  file: %s\n" ,cursorCtrl->currFile->fullName     );
               #endif

               // Advance to the next file in the list.
               cursorCtrl->currFile = cursorCtrl->currFile->next;
            }
         }

         if(NULL != cursorCtrl->currFile && 0 != cursorCtrl->currFile->fileHandle && cursorCtrl->readBufferSize > 0)
         {
            // TODO: Prefetch several records to reduce context switching?
            cursorCtrl->recSize  = parseOutRecord( pVTabCur );

            #ifdef   DEBUG_MODE
//          printf("  cursorCtrl->rowNumber=%d\tcursorCtrl->recNumber=%d\n" ,cursorCtrl->rowNumber ,cursorCtrl->recNumber );
            #endif
         }

      }  while( cursorCtrl->currFile   != NULL  // NOT at the end of file list.
            &&(!cursorCtrl->isEndOfRec ));      // Either have NOT reach end of record OR the very first buffer read.
   }

   DEBUG_APITRACE("Done  getNextRecord() ...\n");
   return   rc;
}


static    inline     // ISO C99
MemValue *setMemValue( MemValue *pMemValue ,const TfrCursor *pTfrCursor ,const int pColID ,const unsigned char pUseSqlite3Free )
{
   register    int   i,j,k;
   unsigned    short fldPos;
   unsigned    short fldLen;
   // Alias them.
   CursorCtrl *cursorCtrl= pTfrCursor->cursorCtrl;
   FileAttr   *fileAttr  = pTfrCursor->cursorCtrl->currFile;
   RecBufCtrl *recBufCtrl= pTfrCursor->cursorCtrl->recBufferCtrl;
   FieldAttr  *fieldAttr = pTfrCursor->cursorCtrl->recBufferCtrl[ pColID -TFR_METACOL_COUNT ].field;

   memset( pMemValue ,'\0' ,sizeof( MemValue ) ); // Initialize the structure to NULL.

   switch( pColID )
   {
      case -1: pMemValue->i       = cursorCtrl->rowNumber;
               pMemValue->dataType= SQLITE_INTEGER;
               break;
      case  0: // RecNum_
               pMemValue->i       = cursorCtrl->recNumber;
               pMemValue->dataType= SQLITE_INTEGER;
               break;
      case  1: // RecSize_
               pMemValue->i       = cursorCtrl->recSize;
               pMemValue->dataType= SQLITE_INTEGER;
               break;
      case  2: // FileSize_
               pMemValue->i       = fileAttr->fileSize;
               pMemValue->dataType= SQLITE_INTEGER;
               break;
      case  3: // FileDate_
               pMemValue->z       = sqlite3_mprintf("%s" ,fileAttr->modifiedOn   );
               pMemValue->n       =                       fileAttr->modifiedOnLen;
               pMemValue->dataType= SQLITE_TEXT;
               break;
      case  4: // FileName_
               pMemValue->z       = sqlite3_mprintf("%s" ,fileAttr->baseName     );
               pMemValue->n       =                       fileAttr->baseNameLen;
               pMemValue->dataType= SQLITE_TEXT;
               break;
      case  5: // FileDir_
               pMemValue->z       = sqlite3_mprintf("%s" ,fileAttr->dirName      );
               pMemValue->n       =                       fileAttr->dirNameLen;
               pMemValue->dataType= SQLITE_TEXT;
               break;
      case  6: // Hostname_
               pMemValue->z       = sqlite3_mprintf("%s" ,cursorCtrl->hostName   );
               pMemValue->n       =                       cursorCtrl->hostNameLen;
               pMemValue->dataType= SQLITE_TEXT;
               break;
      default: // 6 File metadata virtual column inserted by this module for the VTable definition.
               fldPos = recBufCtrl[ pColID -TFR_METACOL_COUNT ].startPos;
               fldLen = recBufCtrl[ pColID -TFR_METACOL_COUNT ].length;

               if(0 == fldLen )  pMemValue->dataType= SQLITE_NULL;   // Zero length is NULL.
               else{
                  char          *txtVal=  NULL;
                  char          *endPtr=  NULL;
                  double         rVal;
                  sqlite3_int64  lVal;

                  txtVal  =  substrdecode( cursorCtrl->recBuffer ,fldPos ,fldLen ,fieldAttr->charEncoding );

                  for(i=0 ; i < fieldAttr->mapCount ; i++)
                  {
                     // ONLY exact match.
                     if((fldLen == fieldAttr->mapList[i].mapFromLen) && (0 == strcmp( txtVal ,fieldAttr->mapList[i].mapFrom )))
                     {
                        free( txtVal );

                        txtVal = fieldAttr->mapList[i].mapTo;
                        fldLen = fieldAttr->mapList[i].mapToLen;
                        if( 0 == fieldAttr->mapList[i].mapToLen ) pMemValue->dataType= SQLITE_NULL;
                        break;
                     }
                  }

                  if(NULL == txtVal)
                     pMemValue->dataType= SQLITE_NULL;
                  else
                  {
                     pMemValue->dataType= fieldAttr->dataType;

                     switch( pMemValue->dataType )
                     {
                        case  SQLITE_NULL:
                              break;
                        case  SQLITE_INTEGER:
                              lVal  = strtol( txtVal ,&endPtr ,0 );
                              if('\0' == *endPtr && endPtr != txtVal )
                              {
                                 pMemValue->i       = lVal;
                              }else{
                                 pMemValue->z       = sqlite3_mprintf("%s" ,txtVal);
                                 pMemValue->n       = fldLen;
                                 pMemValue->dataType= SQLITE_TEXT;
                              }
                              break;
                        case  SQLITE_FLOAT:
                              rVal  = strtod( txtVal ,&endPtr );
                              if('\0' == *endPtr && endPtr != txtVal )
                              {
                                 pMemValue->r       = rVal;
                              }else{
                                 pMemValue->z       = sqlite3_mprintf("%s" ,txtVal);
                                 pMemValue->n       = fldLen;
                                 pMemValue->dataType= SQLITE_TEXT;
                              }
                              break;
                        case  SQLITE_BLOB:   // TODO: Finish this implementation.
                              break;
                        case  SQLITE_TEXT:
                        default:
                                 pMemValue->z       = sqlite3_mprintf("%s" ,txtVal);
                                 pMemValue->n       = fldLen;
                              break;
                     }
                  }
                  free( txtVal );
               }

               break;
   }

   return   pMemValue;
}


// SQLite3 Interface routines.

static
int   xConnect( sqlite3 *db ,void *pAux ,int pArgc ,const char * const *pArgv ,sqlite3_vtab **ppVTable ,char **pzErrMsg )
{
   DEBUG_APITRACE("In   xConnect() ...\n");

   int   i ,j ,k ,rc    =  SQLITE_OK;
   TfrTable   *tfrTable =  NULL;
              *ppVTable =  NULL;

   // Used for creating the backing table using *db.  Not needed.
   // argv[0]  =  Module name; "tfr"
   // argv[1]  =  Logical name of database.
   // argv[2]  =  User provided table name for the virtual table.
   // argv[3]  =  User argument 1.

   if( pArgc > 3 )
   {
     *pzErrMsg =  NULL;

      tfrTable = sqlite3_malloc(sizeof(TfrTable) );
      if(NULL != tfrTable )
      {
         memset( tfrTable ,'\0' ,sizeof(TfrTable));   // Initialize the structure to NULL.

         tfrTable->dsa  =  sqlite3_malloc(sizeof(DatasetAttr));

         if(NULL == tfrTable->dsa ) (*pzErrMsg) =  sqlite3_mprintf("TFRERR: Allocating memory for DatasetAttr struct!" );
         else{
            tfrTable->dsa  =  setDataSetAttr( tfrTable->dsa ,pArgc ,pArgv ,*pzErrMsg );  // Initialize the DataSetAttr struct.
           *ppVTable       = (sqlite3_vtab *) tfrTable;

            #ifdef SHOW_VTABDEF
            displayVTable( *ppVTable );
            #endif

            if(NULL !=*pzErrMsg )
            {
               freeDataSetAttr( tfrTable->dsa );
                                tfrTable->dsa= NULL;
               sqlite3_free(    tfrTable->dsa );

               rc = SQLITE_ERROR;
            }
         }
      }else{
         rc       =  SQLITE_NOMEM;
        *pzErrMsg =  sqlite3_mprintf("TFRERR: Allocating memory for TfrTable struct!" );
      }
   }else{
      rc       =  SQLITE_ERROR;
     *pzErrMsg =  sqlite3_mprintf("TFRERR: Insufficient arguments supplied!");
   }

   DEBUG_APITRACE("Done xConnect() ...\n");
   return   rc;
}

static
int   xDisconnect( sqlite3_vtab *pVTable )
{
   DEBUG_APITRACE("In   xDisconnect() ...\n");
   TfrTable   *tfrTable = (TfrTable *)pVTable;

   sqlite3_free( tfrTable );

   DEBUG_APITRACE("Done xDisconnect() ...\n");
   return   SQLITE_OK;
}

static
int   xCreate(  sqlite3 *db ,void *pAux ,int pArgc ,const char * const *pArgv ,sqlite3_vtab **ppVTable ,char **pzErrMsg )
{
   int   rc =  SQLITE_OK;
   rc =  xConnect( db ,pAux ,pArgc ,pArgv ,ppVTable ,pzErrMsg );

   if(SQLITE_OK == rc )
   {
      char *sqlStr= getVTableDDL( *ppVTable   );
      #if 0
      printf("%s\n",sqlStr);
      #endif
      rc =  sqlite3_declare_vtab(  db ,sqlStr );

      if(SQLITE_OK != rc ) *pzErrMsg = sqlite3_mprintf("TFRERR: Unable to declare virtual table!");

      sqlite3_free( sqlStr );
   }

   SHOW_USEDMEMORY("Done xCreate() ...\n");
   DEBUG_APITRACE( "Done xCreate() ...\n");
   return   rc;
}

static
int   xRename(  sqlite3_vtab *pVTable ,const char *pzNewName )
{
   DEBUG_APITRACE("In   xRename() ...\n");
   TfrTable   *tfrTable = (TfrTable *)pVTable;


   DEBUG_APITRACE("Done xRename() ...\n");
   return   SQLITE_OK;
}

static
int   xDestroy( sqlite3_vtab *pVTable  )
{
   DEBUG_APITRACE("In   xDestroy() ...\n");

   TfrTable   *tfrTable = (TfrTable *)pVTable;

   freeDataSetAttr(tfrTable->dsa );
   sqlite3_free( tfrTable );

   SHOW_USEDMEMORY("Done xDestroy() ...\n");
   DEBUG_APITRACE( "Done xDestroy() ...\n");
   return   SQLITE_OK;
}

static
int   xOpen(    sqlite3_vtab *pVTable ,sqlite3_vtab_cursor **ppVTabCur )
{
   DEBUG_APITRACE( "In   xOpen() ...\n");
   SHOW_USEDMEMORY("In   xOpen() ...\n");

   int         rc       =  SQLITE_OK;
   char       *pzErrMsg =  NULL;
   TfrTable   *tfrTable = (TfrTable *)pVTable;
   TfrCursor  *tfrCursor=  NULL;

   tfrCursor = sqlite3_malloc(sizeof(TfrCursor));

   if(NULL != tfrCursor )
   {
      tfrCursor->cursorCtrl = newCursorCtrl(          tfrTable->dsa ,pzErrMsg );    // Initialize the CursorCtrl struct.
     *ppVTabCur             =(sqlite3_vtab_cursor *)  tfrCursor;

      if(NULL != pzErrMsg )   freeCursorCtrl(   tfrCursor->cursorCtrl   );
   }else{
      rc       =  SQLITE_NOMEM;
//    pzErrMsg =  sqlite3_mprintf("TFRERR: Allocating memory for TfrCursor struct!" );
      sqlite3_log(SQLITE_NOMEM   ,"TFRERR: Allocating memory for TfrCursor struct!");
   }

   SHOW_USEDMEMORY("Done xOpen() ...\n");
   DEBUG_APITRACE( "Done xOpen() ...\n");
   return   rc;
}

static
int   xClose(   sqlite3_vtab_cursor *pVTabCur )
{
   SHOW_USEDMEMORY("In   xClose() ...\n");
   DEBUG_APITRACE( "In   xClose() ...\n");

   TfrCursor  *tfrCursor = (TfrCursor *)pVTabCur;

   freeCursorCtrl( tfrCursor->cursorCtrl  );
   sqlite3_free(   tfrCursor              );

   SHOW_USEDMEMORY("Done xClose() ...\n");
   DEBUG_APITRACE( "Done xClose() ...\n");
   return   SQLITE_OK;
}

static
int   xEof(     sqlite3_vtab_cursor *pVTabCur )
{
   DEBUG_APITRACE("In   xEof() ...");

   TfrCursor *tfrCursor = (TfrCursor *)pVTabCur;

   #ifdef   DEBUG_MODE
   printf("\t%s\n" ,(NULL == tfrCursor->cursorCtrl->currFile) ?  "True " : "False"  );
   #endif

   return   (NULL == tfrCursor->cursorCtrl->currFile);
}

#ifdef   DEBUG_MODE
static   unsigned char  doXColumn;
#endif
static
int   xNext(    sqlite3_vtab_cursor *pVTabCur )
{
   DEBUG_APITRACE("In   xNext() ...\n");

   int   rc =  SQLITE_OK;
   char  skip;

   TfrCursor  *tfrCursor = (TfrCursor *)pVTabCur;
   CursorCtrl *cursorCtrl=  tfrCursor-> cursorCtrl;

   do{
      rc  =    getNextRecord( pVTabCur );

      skip=(0< cursorCtrl->hdrSkipped || 0< cursorCtrl->recSkipped); // Need one extra iteration to get to the next displayable record.

      if(0< cursorCtrl->recSkipped  && cursorCtrl->hdrSkipped ==0 )  // Skip records ONLY after headers are skipped.
            cursorCtrl->recSkipped--;

      if(0< cursorCtrl->hdrSkipped)
      {     cursorCtrl->hdrSkipped--;

         if(0< cursorCtrl->hdrSkipped) cursorCtrl->rowNumber--;
         if(0==cursorCtrl->hdrSkipped) cursorCtrl->rowNumber--;      // To account for the extra skip iteration.
      }
   }  while(NULL != cursorCtrl->currFile                             // Not end of dataset files.
      &&(  (0< cursorCtrl->hdrSkipped || 0< cursorCtrl->recSkipped)  // Still have stuff to skip.
         || 1==skip)                                                 // One extra iteration to do.
      );

   #ifdef   DEBUG_MODE
   doXColumn = 1;
   #endif

   DEBUG_APITRACE("Done xNext() ...\n");
   return   rc;
}

static
int   xColumn(  sqlite3_vtab_cursor *pVTabCur ,sqlite3_context *pContext ,int pColID )
{
   #ifdef   DEBUG_MODE
   if(doXColumn ) {
      doXColumn = 0;
      printf("In   xColumn .. first colID=%d\n" ,pColID );
   }
   #endif
// DEBUG_APITRACE("In   xColumn ...\n")

   TfrCursor   *tfrCursor = (TfrCursor *)pVTabCur;
   MemValue     memValue;

   setMemValue(&memValue ,tfrCursor ,pColID ,1 );  // TODO: Convert to memcpy().

   switch( memValue.dataType )
   {
      case  SQLITE_INTEGER:
            sqlite3_result_int64(   pContext ,memValue.i );
            break;
      case  SQLITE_FLOAT:
            sqlite3_result_double(  pContext ,memValue.r );
            break;
      case  SQLITE_TEXT:
            sqlite3_result_text(    pContext ,memValue.z ,memValue.n ,sqlite3_free );
            break;
      case  SQLITE_BLOB:
            sqlite3_result_zeroblob(pContext ,0 ); // TODO: Implement this.
            break;
      case  SQLITE_NULL:
      default:
            sqlite3_result_null(    pContext    );
   }

// DEBUG_APITRACE("Done xColumn ...\n")
   return SQLITE_OK;
}

static
int   xRowid(   sqlite3_vtab_cursor *pVTabCur ,sqlite_int64 *pRowID )
{
   DEBUG_APITRACE("In   xRowid ...\n")

   TfrCursor  *tfrCursor = (TfrCursor *)pVTabCur;

  *pRowID = tfrCursor->cursorCtrl->rowNumber;

   DEBUG_APITRACE("Done xRowid ...\n")
   return   SQLITE_OK;
}

static
int   xBestIndex( sqlite3_vtab *pVTable ,sqlite3_index_info *pIdxInfo )
{
   DEBUG_APITRACE("In   xBestIndex() ...\n");

   TfrTable   *tfrTable  = (TfrTable *)pVTable;

   DEBUG_APITRACE("Done xBestIndex() ...\n");
   return   SQLITE_OK;
}

static
int   xFilter(  sqlite3_vtab_cursor *pVTabCur ,int pIdxNum ,const char *pIdxStr ,int pArgc ,sqlite3_value **ppArgv )
{
   DEBUG_APITRACE("In   xFilter() ...\n");

   TfrCursor   *tfrCursor = (TfrCursor *)pVTabCur;
// DatasetAttr *dsa       =((TfrTable  *)pVTabCur->pVtab)->dsa;

   resetCursorCtrl( tfrCursor->cursorCtrl );

   xNext( pVTabCur );

   DEBUG_APITRACE("Done xFilter() ...\n");
   return   SQLITE_OK;
}

// End of SQLite3 Interface routines.

//
// A virtual table module that scan structure text file(s).
//
static   sqlite3_module tfrModule = {
    1             // iVersion
                  // Table instance functions.
   ,xCreate       // xCreate     - Required. Called when a virtual table instance is first created with the CREATE VIRTUAL TABLE command.
   ,xConnect      // xConnect    - Required, but frequently the same as xCreate(). This is called when a database with an existing virtual table instance is loaded. Called once for each table instance.
   ,xBestIndex    // xBestIndex  - Required. Called, sometimes several times, when the database engine is preparing an SQL statement that involves a virtual table.
   ,xDisconnect   // xDisconnect - Required. Called when a database containing a virtual table instance is detached or closed. Called once for each table instance.
   ,xDestroy      // xDestroy    - Required, but frequently the same as xDisconnect(). This is called when a virtual table instance is destroyed with the DROP TABLE command.
                  // Cursor functions.
   ,xOpen         // xOpen       - Required. Called to create and initialize a table cursor.
   ,xClose        // xClose      - Required. Called to shut down and release a table cursor.
   ,xFilter       // xFilter     - Required. Called to initiate a table scan and provide information about any specific conditions put on this particular table scan.
   ,xNext         // xNext       - Required. Called to advance a table cursor to the next row.
   ,xEof          // xEof        - Required. Called to see if a table cursor has reached the end of the table or not. This function is always called right after a call to xFilter() or xNext().
   ,xColumn       // xColumn     - Required. Called to extract a column value for the current row. Normally called multiple times per row.
   ,xRowid        // xRowid      - Required. Called to extract the virtual ROWID of the current row.
                  //
   ,0             // xUpdate
                  // Transaction control functions.
   ,0             // xBegin
   ,0             // xSync
   ,0             // xCommit
   ,0             // xRollback
                  //
   ,0             // xFindFunction
   ,xRename       // xRename     - Required. Called when a virtual table is renamed using the ALTER TABLE...RENAME command.
   ,0             // xSavepoint
   ,0             // xRelease
   ,0             // xRollbackTo
};

// Extension load function entry point.
//
int   tfr_init(sqlite3 *db ,char **ppzErrMsg ,const sqlite3_api_routines *pApi )
{
      int    i;
      char  *moduleName =  substr(__FILE__ ,0 ,strlen(__FILE__)-2 );

      SQLITE_EXTENSION_INIT2( pApi );

      printf("Authored by: Edward Lau (elau1004@netscape.net)\n");
      printf("Module \"%s\" registered. Compiled on %s %s\n"   ,moduleName ,__DATE__ ,__TIME__ );

      return   sqlite3_create_module( db ,moduleName ,&tfrModule ,NULL );
}

// Default entry point which SQLite3 will look for when this module is loaded.
int   sqlite3_extension_init(    sqlite3 *db ,char **ppzErrMsg ,const sqlite3_api_routines *pApi )
{
      return   tfr_init( db ,ppzErrMsg ,pApi );
}
