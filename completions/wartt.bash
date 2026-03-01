_wartt_completion() {
    local cur prev cmd opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    cmd="${COMP_WORDS[1]}"

    if [[ ${COMP_CWORD} -eq 1 ]]; then
        opts="ingest-openclaw"
        COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
        return 0
    fi

    case "${prev}" in
        --source|--state-file)
            COMPREPLY=( $(compgen -f -- "${cur}") )
            return 0
            ;;
    esac

    case "${cmd}" in
        ingest-openclaw)
            opts="--once --source --help -h"
            ;;
        *)
            opts=""
            ;;
    esac

    COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
}

complete -F _wartt_completion wartt
