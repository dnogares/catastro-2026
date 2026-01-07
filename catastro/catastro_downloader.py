import time
import zipfile
import requests
from pathlib import Path


class CatastroDownloader:
    """
    Descargador robusto de datos catastrales
    preparado para servidores cloud (Easypanel, Docker, etc.)
    """

    BASE_URL = "https://www.sedecatastro.gob.es"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "es-ES,es;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://www.sedecatastro.gob.es/",
    }

    def __init__(self, output_dir: str, retries: int = 3, timeout: int = 20):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.retries = retries
        self.timeout = timeout

    # =====================================================
    # MÉTODO PRINCIPAL
    # =====================================================

    def descargar_todo_completo(self, referencia: str):
        """
        Descarga y empaqueta toda la información de una referencia
        """
        ref = referencia.strip().upper()
        ref_dir = self.output_dir / ref
        zip_path = self.output_dir / f"{ref}_completo.zip"

        # 🧠 CACHE: si ya existe el ZIP, no vuelvas a descargar
        if zip_path.exists():
            print(f"ℹ️ Cache detectada para {ref}")
            return True, str(zip_path)

        ref_dir.mkdir(parents=True, exist_ok=True)

        try:
            gml_ok = self._descargar_gml(ref, ref_dir)
            if not gml_ok:
                return False, None

            # Empaquetar
            self._crear_zip(ref_dir, zip_path)
            return True, str(zip_path)

        except Exception as e:
            print(f"❌ Error descargando {ref}: {e}")
            return False, None

    # =====================================================
    # DESCARGA GML
    # =====================================================

    def _descargar_gml(self, referencia: str, ref_dir: Path) -> bool:
        """
        Descarga el GML de parcela
        """
        gml_dir = ref_dir / "gml"
        gml_dir.mkdir(parents=True, exist_ok=True)

        gml_path = gml_dir / f"{referencia}_parcela.gml"

        if gml_path.exists():
            print(f"ℹ️ GML ya existe: {gml_path}")
            return True

        url = (
            f"{self.BASE_URL}/Accesos/SECAccesos.aspx?"
            f"RC={referencia}&tipo=parcelas"
        )

        for intento in range(1, self.retries + 1):
            try:
                print(f"⬇️ Descargando GML ({intento}/{self.retries})")

                r = requests.get(
                    url,
                    headers=self.HEADERS,
                    timeout=self.timeout
                )

                if r.status_code != 200 or not r.content:
                    raise ConnectionError(
                        f"Respuesta inválida ({r.status_code})"
                    )

                gml_path.write_bytes(r.content)
                print(f"✅ GML guardado en {gml_path}")
                return True

            except Exception as e:
                print(f"⚠️ Intento {intento} fallido: {e}")
                time.sleep(2 + intento * 2)

        print("❌ No se pudo descargar el GML (posible bloqueo Catastro)")
        return False

    # =====================================================
    # ZIP
    # =====================================================

    def _crear_zip(self, carpeta: Path, zip_path: Path):
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file in carpeta.rglob("*"):
                if file.is_file():
                    zipf.write(file, arcname=file.relative_to(carpeta))

        print(f"📦 ZIP creado: {zip_path}")
