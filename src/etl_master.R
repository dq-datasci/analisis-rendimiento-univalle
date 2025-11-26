library(tidyverse)
library(readxl)
library(DBI)
library(RPostgres)
library(janitor)
library(writexl)

# --- 1. CONEXIÓN A SUPABASE ---
con <- dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("SUPABASE_HOST"),
  dbname = Sys.getenv("SUPABASE_DB"),
  user = Sys.getenv("SUPABASE_USER"),
  password = Sys.getenv("SUPABASE_PASSWORD"),
  port = Sys.getenv("SUPABASE_PORT")
)
print("✅ Conectado a Supabase.")

# --- 2. LECTURA DEL EXCEL DIRECTO ---
ruta_archivo <- "data/Encuesta a Estudiantes - UNIVALLE(1-186).xlsx"
print(paste("🔄 Leyendo archivo Excel:", ruta_archivo))
df_raw <- read_excel(ruta_archivo)

# --- 3. TRANSFORMACIÓN INTELIGENTE ---

convertir_si_no <- function(x) {
  if(is.numeric(x)) return(x)
  limpio <- str_to_lower(str_trim(x))
  valores_unicos <- unique(na.omit(limpio))
  
  if (length(valores_unicos) == 0) return(x)
  
  es_booleana <- all(valores_unicos %in% c("sí", "no", "si"))
  
  if (es_booleana) {
    return(limpio %in% c("sí", "si")) 
  } else {
    return(x)
  }
}

df_clean <- df_raw %>%
  clean_names() %>% 
  select(
    # Demográficos
    demo_01_sexo            = matches("cual_es_tu_sexo"),
    demo_02_carrera         = matches("que_carrera_estudias"),
    demo_03_edad            = matches("cual_es_tu_edad"),
    demo_04_semestre        = matches("en_que_semestre"),
    demo_05_escolaridad_padre = matches("escolaridad.*padre"),
    demo_06_escolaridad_madre = matches("escolaridad.*madre"),
    demo_07_ingresos        = matches("ingresos_familiares"),
    demo_08_afecta_economia = matches("situacion_economica"),
    demo_09_trabaja         = matches("trabajas_y_estudias"),
    
    # Rendimiento
    rend_01_promedio           = matches("promedio_academico"),
    rend_02_otra_carrera       = matches("estudias_alguna_otra"),
    rend_03_satisfecho         = matches("satisfecho_con_la_carrera"),
    rend_04_materias_completas = matches("te_inscribiste_a_todas"),
    rend_05_beca               = matches("tienes_algun_tipo_de_beca"),
    
    # Uso Cuantitativo
    uso_01_horas_dia          = matches("cuantas_horas_pasas"),
    uso_02_veces_revisa_clase = matches("cuantas_veces_por_hora"),
    
    # Usos Específicos (Checkboxes)
    uso_03_llamadas = matches("^llamadas$"),
    uso_03_mensajes = starts_with("mensajes_de_texto"),
    uso_03_redes    = matches("^redes_sociales$"),
    uso_03_juegos   = matches("^juegos$"),
    uso_03_lectura  = matches("lectura_estudiar"),
    uso_03_internet = matches("^internet$"),
    uso_03_ia       = matches("inteligencia_artificial"),
    
    # Likert (1-5)
    uso_04_distraccion_redes = matches("mayor_distraccion"),
    uso_05_pierde_hilo       = matches("pierdo_el_hilo"),
    uso_06_herramienta_acad  = matches("herramienta_academica"),
    uso_07_revisar_vibracion = matches("revisar_mi_telefono"),
    
    # Indicadores Psicológicos (Sí/No)
    uso_08_necesidad_constante = matches("necesidad_de_utilizar"),
    uso_09_ansiedad_bateria    = matches("bateria_de_su_celular"),
    uso_10_uso_restringido     = matches("uso_se_encuentra_restringido"),
    uso_11_celular_cerca       = matches("celular_cerca"),
    uso_12_ansiedad_sin_celular= matches("invariablemente_ansioso"),
    uso_13_incomodidad_sin_celular = matches("se_siente_incomodo")
  ) %>%
  # 1. Convertir textos de Sí/No a Booleanos
  mutate(across(everything(), convertir_si_no)) %>%
  
  # 2. --- CORRECCIÓN DE TIPOS NUMÉRICOS (Explicit Casting) ---
  mutate(
    # Variables ENTERAS (Sin decimales)
    across(c(demo_03_edad, 
             demo_04_semestre, 
             uso_04_distraccion_redes,
             uso_05_pierde_hilo, 
             uso_06_herramienta_acad, 
             uso_07_revisar_vibracion), as.integer),
    
    # Variables NUMÉRICAS (Pueden tener decimales o ser montos grandes)
    across(c(demo_07_ingresos, 
             rend_01_promedio, 
             uso_01_horas_dia, 
             uso_02_veces_revisa_clase), as.numeric)
  )

print(paste("✨ Datos limpios:", nrow(df_clean), "filas."))

# --- 4. CARGA A SUPABASE (Protegida) ---
tryCatch({
  # TRUNCATE vacía la tabla pero mantiene la estructura (UUIDs, tipos de datos)
  dbExecute(con, "TRUNCATE TABLE encuesta_estudiantes;")
  print("🧹 Tabla limpiada (TRUNCATE) para evitar duplicados.")
  
  # Insertamos los datos nuevos
  dbWriteTable(con, "encuesta_estudiantes", df_clean, append = TRUE, row.names = FALSE)
  print("🚀 ¡Datos actualizados en Supabase!")
}, error = function(e) {
  print("❌ Error en base de datos:")
  print(e)
})

dbDisconnect(con)

# --- 5. EXPORTAR SNAPSHOT PARA EQUIPO ---
ruta_salida <- "output/datos_limpios_para_equipo.xlsx"
write_xlsx(df_clean, ruta_salida)
print(paste("📦 Snapshot Excel generado en:", ruta_salida))
