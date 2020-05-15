//Ronn Joseph, Akilnath Bodipudi 
//File takes around 7 minutes to find plaintext

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <openssl/evp.h>

 unsigned char targethash[]={0xce,0xbd,0xc0,0x66,0x32,0xde,0x5d,0xcd,0x72,0x19,0x41,0x52,0x64,0xd4,0xbe,0x25};
EVP_MD_CTX *mdctx;
unsigned char md_value[EVP_MAX_MD_SIZE];
const EVP_MD *md;
unsigned int md_len, i;

unsigned char *MD5Hashing (unsigned char InputValue[])
{
        mdctx = EVP_MD_CTX_new();
         EVP_DigestInit_ex(mdctx, md, NULL);
         EVP_DigestUpdate(mdctx, InputValue, strlen(InputValue));
         EVP_DigestFinal_ex(mdctx, md_value, &md_len);
         EVP_MD_CTX_free(mdctx);
	return md_value;
}

int compare_hashes(unsigned char InputValue[])
{

int matchflag=1;
unsigned char *genhash=MD5Hashing(InputValue);
	for (int i=0; i<16; i++)
	{
		if(targethash[i]!=genhash[i])
		{
			break;
		}		
		else			
		{
			if (i==15)
			{
			matchflag=0;	
			break;				
			}		
		}
	}
return matchflag;
}

int main(int argc, char *argv[])
{
    printf("Given MD5 hashed value: ");
      for(int i=0; i<16; i++)
      {
        printf("%02x",targethash[i]);
      }
      printf("\n");

     unsigned char GeneratedValue[4]="";

     if (argv[1] == NULL) 
	{
         printf("Usage: mdtest digestname\n");
         exit(1);
	}
    md = EVP_get_digestbyname(argv[1]);
     if (md == NULL) 
	{
         printf("Unknown message digest %s\n", argv[1]);
         exit(1);
        }
        for (int i = 0; i <= 255; i++)
        {
        GeneratedValue[3]=i;
		 if (compare_hashes(GeneratedValue)== 0 )
		{
                break; 
		}
		for (int j = 0; j <= 255; j++)
		{
			GeneratedValue[2]=j;
			if (compare_hashes(GeneratedValue)== 0 )
			{
			break; 
			}
			for (int k = 0; k <= 255; k++)
			{
				GeneratedValue[1]=k;
				 if (compare_hashes(GeneratedValue)== 0 )
				{
				break; 
				}
				for (int l=  0; l <= 255; l++)
				{
			     		GeneratedValue[0]=l;
					if (compare_hashes(GeneratedValue)== 0 )
					{
					break;
					}
				}
				if (compare_hashes(GeneratedValue)== 0 )
				{
				break;
				}
			}
			if (compare_hashes(GeneratedValue)== 0 )
			{
			break;
			}
		}
		if (compare_hashes(GeneratedValue)== 0 )
		{
		break;
		}
        }

	printf("Match Found. Original value = %s\n",GeneratedValue);
            FILE * fpointer = fopen("search1value.txt","w");//write file (create)
	for(int i=0;i<4;i++)
            fprintf(fpointer,"%02x",GeneratedValue[i]);
		fprintf(fpointer,"\n");
            fclose(fpointer);

exit(0);
    return 0;
}
