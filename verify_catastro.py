import sys
import os
from pathlib import Path
from catastro.catastro_downloader import CatastroDownloader

def test_downloader():
    rc = "4528102VK3742N0001PI"
    output_dir = "outputs_test"
    
    # Ensure we use the right path if we are running from root
    sys.path.append(os.path.abspath("."))
    
    import logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    downloader = CatastroDownloader(output_dir=output_dir)
    
    print(f"--- Probando RC: {rc} ---")
    
    # Pruebas de métodos individuales
    print(f"Limpiar referencia: {downloader.limpiar_referencia(rc)}")
    print(f"Extraer Del/Mun: {downloader.extraer_del_mun(rc)}")
    
    # Prueba de descarga completa
    exito, zip_path, pixel_data = downloader.descargar_todo_completo(rc)
    
    print(f"\nResultado final: {'EXITO' if exito else 'FALLO'}")
    print(f"ZIP path: {zip_path}")
    print(f"Pixel Data: {pixel_data}")
    
    if exito:
        ref_dir = Path(output_dir) / rc
        print("\nVerificando archivos generados:")
        for d in ["gml", "images", "pdf"]:
            dir_path = ref_dir / d
            if dir_path.exists():
                files = list(dir_path.glob("*"))
                print(f"  {d}: {len(files)} archivos")
                for f in files:
                    print(f"    - {f.name}")
            else:
                print(f"  {d}: NO EXISTE")

if __name__ == "__main__":
    test_downloader()
