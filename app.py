import os
import time

from fastapi import FastAPI, File, status, UploadFile
from fastapi.responses import JSONResponse
import uvicorn

from utils.file_parser import FileParser


app = FastAPI()

@app.post('/file/upload')
def upload_resume(
    file: UploadFile = File(...)
) -> JSONResponse:
    _hex = hex(round(time.time() * 1e7))[1:]
    ext = file.filename.split('.')[-1]
    
    try:
        contents = file.file.read()
        write_path = f'{_hex}.{ext}'
        with open(write_path, 'wb') as f:
            f.write(contents)
            
        data = FileParser.parse_file(write_path)

        # -----------------------
        # insert to postgres here
        # -----------------------
        for tab in data:
            print(tab)
            print(data[tab])

        os.remove(write_path)

        return JSONResponse(
            content=_hex, 
            status_code=status.HTTP_200_OK
        )
    except ValueError as e:
        return JSONResponse(
            content=str(e), 
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )
    except Exception:
        return JSONResponse(
            content='There was an error uploading the file.', 
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )
    finally:
        file.file.close()


if __name__ == '__main__':
    uvicorn.run('app:app', host='0.0.0.0', port=10000)