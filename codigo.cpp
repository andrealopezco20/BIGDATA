#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <unordered_map>
#include <cctype>
#include <vector>
#include <algorithm>
#include <memory>
#include <Windows.h>
#include <thread>
#include <mutex>
#include <ctime>
#include <direct.h>

// Constantes
const size_t TAMANO_OBJETIVO = 20ULL * 1024 * 1024 * 1024; // 20GB
const int NUM_HILOS = std::thread::hardware_concurrency(); // Nnum de nucles CPU


std::unordered_map<std::string, size_t> contador_palabras_global; // Mapa global de conteo
std::mutex mutex_combinacion;

//obtner direccion del archivo
std::string obtener_directorio(const std::string& ruta) {
    size_t pos = ruta.find_last_of("\\/");
    return (pos == std::string::npos) ? "" : ruta.substr(0, pos);
}

// nombre dle archvo (.txt)
std::string obtener_nombre_archivo(const std::string& ruta) {
    size_t pos = ruta.find_last_of("\\/");
    return (pos == std::string::npos) ? ruta : ruta.substr(pos + 1);
}

// Función optimizada para caracteres de palabra, para deptermianr si un cardcter e sun paeralaba axceptad
inline bool es_caracter_palabra(char c) {
    return (c >= 'a' && c <= 'z') ||
        (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') ||
        c == '\'' || c == '-' ||
        static_cast<unsigned char>(c) > 127;
}

// Duplicar archivo a 20GB de forma eficiente
void expandir_a_20gb(const std::string& ruta_entrada, const std::string& ruta_salida) {
    auto inicio = std::chrono::high_resolution_clock::now();

    std::ifstream entrada(ruta_entrada, std::ios::binary);
    if (!entrada) {
        std::cerr << "Error al abrir el archivo de entrada" << std::endl;
        return;
    }

    // Obtener tamaño original
    entrada.seekg(0, std::ios::end);
    size_t tamano_original = entrada.tellg();
    entrada.seekg(0, std::ios::beg);

    if (tamano_original == 0) {
        std::cerr << "El archivo de entrada está vacío" << std::endl;
        return;
    }

    // Crear buffer de lectura
    std::unique_ptr<char[]> buffer(new char[tamano_original]);
    entrada.read(buffer.get(), tamano_original);

    std::ofstream salida(ruta_salida, std::ios::binary | std::ios::trunc);
    if (!salida) {
        std::cerr << "Error al crear el archivo de salida" << std::endl;
        return;
    }

    // Calcular repeticiones necesarias
    size_t repeticiones = TAMANO_OBJETIVO / tamano_original + 1;
    size_t total_escrito = 0;

    std::cout << "Generando archivo de 20GB..." << std::endl;
    for (size_t i = 0; i < repeticiones && total_escrito < TAMANO_OBJETIVO; ++i) {
        salida.write(buffer.get(), tamano_original);
        total_escrito += tamano_original;

        // Mostrar progreso
        if (i % 10 == 0) {
            double progreso = (double)total_escrito / TAMANO_OBJETIVO * 100;
            std::cout << "\rProgreso: " << progreso << "%" << std::flush;
        }
    }

    auto fin = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duracion = fin - inicio;

    std::cout << "\nArchivo generado: " << ruta_salida << std::endl;
    std::cout << "Tamaño final: " << total_escrito / (1024.0 * 1024 * 1024) << " GB" << std::endl;
    std::cout << "Tiempo de generación: " << duracion.count() << " segundos" << std::endl;
}

// Función para procesar un fragmento del archivo
void procesar_fragmento(const char* inicio, const char* fin, std::unordered_map<std::string, size_t>& contador_local) {
    std::string palabra_actual;
    palabra_actual.reserve(64); // Reservar espacio para evitar múltiples asignaciones

    for (const char* ptr = inicio; ptr < fin; ++ptr) {
        char c = tolower(*ptr);

        if (es_caracter_palabra(c)) {
            palabra_actual += c;
        }
        else if (!palabra_actual.empty()) {
            contador_local[palabra_actual]++;
            palabra_actual.clear();
        }
    }

    // Procesar última palabra si existe
    if (!palabra_actual.empty()) {
        contador_local[palabra_actual]++;
    }
}

// Conteo de palabras ultra eficiente con memoria mapeada y paralelización
void conteo_rapido_palabras(const std::string& ruta_archivo) {
    auto inicio = std::chrono::high_resolution_clock::now();

    HANDLE archivo = CreateFileA(ruta_archivo.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL,
        OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    if (archivo == INVALID_HANDLE_VALUE) {
        std::cerr << "Error al abrir el archivo" << std::endl;
        return;
    }

    LARGE_INTEGER tamano_archivo;
    GetFileSizeEx(archivo, &tamano_archivo);
    std::cout << "Procesando archivo de " << tamano_archivo.QuadPart / (1024.0 * 1024 * 1024) << " GB" << std::endl;

    HANDLE mapeo = CreateFileMapping(archivo, NULL, PAGE_READONLY, 0, 0, NULL);
    if (!mapeo) {
        CloseHandle(archivo);
        std::cerr << "Error al crear mapeo de memoria" << std::endl;
        return;
    }

    const char* datos_archivo = (const char*)MapViewOfFile(mapeo, FILE_MAP_READ, 0, 0, 0);
    if (!datos_archivo) {
        CloseHandle(mapeo);
        CloseHandle(archivo);
        std::cerr << "Error al mapear archivo" << std::endl;
        return;
    }

    // Dividir el trabajo entre hilos
    std::vector<std::thread> hilos;
    std::vector<std::unordered_map<std::string, size_t>> resultados_hilos(NUM_HILOS);
    size_t tamano_fragmento = tamano_archivo.QuadPart / NUM_HILOS;

    std::cout << "Procesando con " << NUM_HILOS << " hilos..." << std::endl;

    for (int i = 0; i < NUM_HILOS; ++i) {
        const char* inicio_fragmento = datos_archivo + (i * tamano_fragmento);
        const char* fin_fragmento = (i == NUM_HILOS - 1) ? datos_archivo + tamano_archivo.QuadPart : inicio_fragmento + tamano_fragmento;

        // Asegurar que no dividamos palabras
        if (i > 0) {
            while (inicio_fragmento < fin_fragmento && es_caracter_palabra(*inicio_fragmento)) {
                inicio_fragmento++;
            }
        }

        hilos.emplace_back(procesar_fragmento, inicio_fragmento, fin_fragmento, std::ref(resultados_hilos[i]));
    }

    // Esperar a que todos los hilos terminen
    for (auto& hilo : hilos) {
        hilo.join();
    }

    // Combinar resultados
    std::cout << "Combinando resultados..." << std::endl;
    for (auto& contador_local : resultados_hilos) {
        for (const auto& par : contador_local) {
            contador_palabras_global[par.first] += par.second;
        }
    }

    // Calcular total de palabras
    size_t total_palabras = 0;
    for (const auto& par : contador_palabras_global) {
        total_palabras += par.second;
    }

    UnmapViewOfFile(datos_archivo);
    CloseHandle(mapeo);
    CloseHandle(archivo);

    auto fin = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duracion = fin - inicio;

    // Mostrar resultados en consola
    std::cout << "\n=== Resultados ===" << std::endl;
    std::cout << "Tiempo total: " << duracion.count() << " segundos" << std::endl;
    std::cout << "Total palabras: " << total_palabras << std::endl;
    std::cout << "Palabras únicas: " << contador_palabras_global.size() << std::endl;

    // Mostrar top 10 palabras
    std::vector<std::pair<std::string, size_t>> top_palabras(contador_palabras_global.begin(), contador_palabras_global.end());
    std::sort(top_palabras.begin(), top_palabras.end(),
        [](const auto& a, const auto& b) { return a.second > b.second; });

    std::cout << "\nTop 10 palabras:" << std::endl;
    for (int i = 0; i < 10 && i < top_palabras.size(); ++i) {
        std::cout << (i + 1) << ". " << top_palabras[i].first << ": " << top_palabras[i].second << std::endl;
    }


    // Guardar resultados en archivo en la misma ruta
    std::string directorio = obtener_directorio(ruta_archivo);
    std::string ruta_resultados = directorio + (directorio.empty() ? "" : "\\") + "resultados_completos_palabras.txt";

    // Determinar si el archivo ya existe
    bool archivo_existe = (GetFileAttributesA(ruta_resultados.c_str()) != INVALID_FILE_ATTRIBUTES);

    std::ofstream archivo_salida(ruta_resultados, std::ios::app); // Modo append

    if (archivo_salida) {
        // Si el archivo ya existía, añadir separación
        if (archivo_existe) {
            archivo_salida << "\n\n========================================\n";
        }

        // Obtener fecha y hora actual
        auto ahora = std::chrono::system_clock::now();
        std::time_t tiempo_actual = std::chrono::system_clock::to_time_t(ahora);
        char buffer_fecha[80];
        ctime_s(buffer_fecha, sizeof(buffer_fecha), &tiempo_actual);

        // Escribir resultados
        archivo_salida << "=== LISTADO COMPLETO DE PALABRAS ===\n";
        archivo_salida << "Fecha: " << buffer_fecha;
        archivo_salida << "Archivo procesado: " << obtener_nombre_archivo(ruta_archivo) << "\n";
        archivo_salida << "Tamaño: " << tamano_archivo.QuadPart / (1024.0 * 1024 * 1024) << " GB\n";
        archivo_salida << "Tiempo de procesamiento: " << duracion.count() << " segundos\n";
        archivo_salida << "Total palabras: " << total_palabras << "\n";
        archivo_salida << "Palabras únicas: " << contador_palabras_global.size() << "\n\n";
        archivo_salida << "LISTADO ALFABÉTICO DE PALABRAS:\n";

        // Ordenar alfabéticamente para el listado completo
        std::vector<std::pair<std::string, size_t>> palabras_ordenadas(
            contador_palabras_global.begin(), contador_palabras_global.end());
        std::sort(palabras_ordenadas.begin(), palabras_ordenadas.end());

        // Escribir todas las palabras con sus frecuencias
        for (const auto& par : palabras_ordenadas) {
            archivo_salida << par.first << ": " << par.second << "\n";
        }

        archivo_salida.close();
        std::cout << "\nListado completo de palabras guardado en: " << ruta_resultados << std::endl;
    }
    else {
        std::cerr << "Error al crear/abrir el archivo de resultados" << std::endl;
    }
}

int main() {
    std::string archivo_entrada = "C:/Users/andre/Downloads/data_bigdata/es.txt";
    std::string archivo_grande = "C:/Users/andre/Downloads/data_bigdata/archivo_20gb.txt";

    // Paso 1: Generar archivo de 20GB
    expandir_a_20gb(archivo_entrada, archivo_grande);

    // Paso 2: Contar palabras
    conteo_rapido_palabras(archivo_grande);

    return 0;
}
