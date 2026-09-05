#!/bin/zsh

set -euo pipefail

ROOT="${0:A:h}"
VERSION="${LBP_PACKAGE_VERSION:-}"
TARGETS="${LBP_BUILD_TARGETS:-darwin/arm64,windows/amd64}"
TARGETS="${TARGETS//[[:space:]]/}"
NO_BUILD=false
LOCAL_ONLY=false
STAGE=""
IDENTITY="${LBP_CODESIGN_IDENTITY:-}"
NOTARY_PROFILE="${LBP_NOTARY_PROFILE:-VaderApp}"

cleanup() {
  if [[ -n "$STAGE" && -d "$STAGE" ]]; then
    /bin/rm -rf -- "$STAGE"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

usage() {
  print -r -- '用法：./pack.command [--no-build] [--local]

建立獨立 macOS DMG 與 Windows MSI，不修改既有部署 ZIP。
  --no-build  沿用指定版本或 dist 中最新安裝版資源，重新封裝
  --local     跳過 Developer ID 簽章與公證，僅供本地使用
  -h, --help  顯示說明

環境變數：
  LBP_PACKAGE_VERSION     版本，例如 1.26.0906 build 1200
  LBP_BUILD_TARGETS       預設 darwin/arm64,windows/amd64
  LBP_CODESIGN_IDENTITY   Developer ID Application 簽章身分
  LBP_NOTARY_PROFILE      notarytool Keychain Profile，預設 VaderApp
  LBP_WIXL                msitools 的 wixl 路徑
  LBP_MSI_SIGN_COMMAND    可選的 MSI 簽章程式，第一個參數為 MSI 路徑

未指定 MSI 簽章程式時產出 -unsigned.msi，不宣稱已簽章。'
}

for argument in "$@"; do
  case "$argument" in
    --no-build) NO_BUILD=true ;;
    --local) LOCAL_ONLY=true ;;
    -h|--help) usage; exit 0 ;;
    *) print -u2 "不支援的參數：$argument"; exit 1 ;;
  esac
done

cd "$ROOT"
[[ -f go.mod && -f src/cmd/loadbalanceprovider/main.go ]] || { print -u2 '專案目錄不完整'; exit 1; }
[[ ! -L "$ROOT/dist" ]] || { print -u2 'dist 不可為符號連結'; exit 1; }
[[ "$(uname -s)" == Darwin ]] || { print -u2 'DMG 簽章及公證必須在 macOS 執行'; exit 1; }
for tool in go hdiutil ditto codesign xcrun shasum; do
  command -v "$tool" >/dev/null || { print -u2 "缺少工具：$tool"; exit 1; }
done
command -v "${LBP_WIXL:-wixl}" >/dev/null || { print -u2 '請先安裝 msitools（wixl），或設定 LBP_WIXL'; exit 1; }
if [[ ",$TARGETS," == *,windows/arm64,* ]]; then
  command -v msibuild >/dev/null || { print -u2 'ARM64 MSI 需要 msibuild'; exit 1; }
fi
[[ ",$TARGETS," == *,darwin/* && ",$TARGETS," == *,windows/* ]] || { print -u2 '至少需要一個 darwin 與一個 windows 目標'; exit 1; }

if [[ "$LOCAL_ONLY" == false ]]; then
  if [[ -z "$IDENTITY" ]]; then
    IDENTITY="$(/usr/bin/security find-identity -v -p codesigning | sed -En 's/^[[:space:]]*[0-9]+\) [0-9A-F]+ "(Developer ID Application:.*)"$/\1/p' | head -1)"
  fi
  [[ -n "$IDENTITY" && "$IDENTITY" != '-' ]] || { print -u2 '缺少 Developer ID Application；本地版本請加 --local'; exit 1; }
  xcrun notarytool history --keychain-profile "$NOTARY_PROFILE" >/dev/null
fi

if [[ "$NO_BUILD" == true && -z "$VERSION" ]]; then
  typeset -a releases
  releases=("$ROOT"/dist/1.*-build-*(/N))
  (( ${#releases} > 0 )) || { print -u2 '沒有可重用的安裝版資源，請先執行 ./pack.command'; exit 1; }
  VERSION="${${releases[-1]:t}/-build-/ build }"
fi
[[ -n "$VERSION" ]] || VERSION="$(TZ=Asia/Taipei date '+1.%y.%m%d build %H%M')"
[[ "$VERSION" =~ '^1\.[0-9]{2}\.[0-9]{4} build [0-9]{4}$' ]] || { print -u2 '版本格式必須為 1.YY.MMDD build HHmm'; exit 1; }
RELEASE_NAME="${VERSION/ build /-build-}"
RELEASE="$ROOT/dist/$RELEASE_NAME"
[[ ! -L "$RELEASE" ]] || { print -u2 '發行目錄不可為符號連結'; exit 1; }
STAGE="$(mktemp -d "${TMPDIR:-/tmp}/lbp-pack.XXXXXX")"
typeset -a arguments
arguments=(-version "$VERSION" -targets "$TARGETS")
[[ "$NO_BUILD" == false ]] || arguments+=(-no-build)
if [[ "$NO_BUILD" == true ]]; then
  # 重封裝會改變 MSI；失敗時不能留下宣稱屬於本輪的舊發行清單。
  rm -f "$RELEASE/PACKAGES-SHA256SUMS" "$RELEASE/SIGNING_STATUS.txt"
fi
go run -buildvcs=false ./src/cmd/packstage "${arguments[@]}"

staple() {
  local target="$1"
  local attempt
  for attempt in {1..10}; do
    if xcrun stapler staple "$target"; then
      xcrun stapler validate "$target"
      return
    fi
    sleep 6
  done
  print -u2 "無法附加公證票根：$target"
  return 1
}

typeset -a packages
packages=()
print -r -- "version=$VERSION" > "$STAGE/SIGNING_STATUS.txt"
for selected in ${(s:,:)TARGETS}; do
  [[ "$selected" == darwin/* ]] || continue
  platform="macos-${selected#darwin/}"
  app="$RELEASE/$platform/LoadBalanceProvider.app"
  image_root="$STAGE/$platform"
  mkdir -p "$image_root"
  ditto "$app" "$image_root/LoadBalanceProvider.app"
  app="$image_root/LoadBalanceProvider.app"
  suffix=""
  if [[ "$LOCAL_ONLY" == true ]]; then
    suffix="-local"
    codesign --force --sign - "$app/Contents/Resources/LoadBalanceProvider"
    codesign --force --sign - "$app"
  else
    codesign --force --options runtime --timestamp --sign "$IDENTITY" "$app/Contents/Resources/LoadBalanceProvider"
    codesign --force --options runtime --timestamp --sign "$IDENTITY" "$app"
    codesign --verify --deep --strict "$app"
    ditto -c -k --sequesterRsrc --keepParent "$app" "$STAGE/$platform-notarize.zip"
    xcrun notarytool submit "$STAGE/$platform-notarize.zip" --keychain-profile "$NOTARY_PROFILE" --wait
    staple "$app"
    /usr/sbin/spctl --assess --type execute "$app"
  fi
  ln -s /Applications "$image_root/Applications"
  name="LoadBalanceProvider-$RELEASE_NAME-$platform$suffix.dmg"
  hdiutil create -volname LoadBalanceProvider -srcfolder "$image_root" -format UDZO -ov "$STAGE/$name"
  if [[ "$LOCAL_ONLY" == false ]]; then
    codesign --force --timestamp --sign "$IDENTITY" "$STAGE/$name"
    codesign --verify --strict "$STAGE/$name"
    xcrun notarytool submit "$STAGE/$name" --keychain-profile "$NOTARY_PROFILE" --wait
    staple "$STAGE/$name"
    /usr/sbin/spctl --assess --type open --context context:primary-signature "$STAGE/$name"
  fi
  mv -f "$STAGE/$name" "$RELEASE/$platform/$name"
  packages+=("$platform/$name")
  print -r -- "$platform/$name: $([[ "$LOCAL_ONLY" == true ]] && print '本地 adhoc，未公證' || print 'Developer ID 簽章及公證完成')" >> "$STAGE/SIGNING_STATUS.txt"
done

for selected in ${(s:,:)TARGETS}; do
  [[ "$selected" == windows/* ]] || continue
  platform="windows-${selected#windows/}"
  name="LoadBalanceProvider-$RELEASE_NAME-$platform-unsigned.msi"
  [[ -f "$RELEASE/$platform/$name" ]] || { print -u2 "缺少 MSI：$name"; exit 1; }
  if [[ -n "${LBP_MSI_SIGN_COMMAND:-}" ]]; then
    command -v osslsigncode >/dev/null || { print -u2 '簽章驗證需要 osslsigncode'; exit 1; }
    cp "$RELEASE/$platform/$name" "$STAGE/$name"
    "$LBP_MSI_SIGN_COMMAND" "$STAGE/$name"
    osslsigncode verify "$STAGE/$name"
    signed_name="${name/-unsigned.msi/.msi}"
    mv -f "$STAGE/$name" "$RELEASE/$platform/$signed_name"
    name="$signed_name"
    status_text='Authenticode 簽章驗證完成'
  else
    status_text='未簽章，Windows 可能提示未知發行者'
  fi
  packages+=("$platform/$name")
  print -r -- "$platform/$name: $status_text" >> "$STAGE/SIGNING_STATUS.txt"
done

: > "$STAGE/PACKAGES-SHA256SUMS"
for relative in "${packages[@]}"; do
  (cd "$RELEASE"; shasum -a 256 "$relative") >> "$STAGE/PACKAGES-SHA256SUMS"
done
mv -f "$STAGE/SIGNING_STATUS.txt" "$RELEASE/SIGNING_STATUS.txt"
mv -f "$STAGE/PACKAGES-SHA256SUMS" "$RELEASE/PACKAGES-SHA256SUMS"
print '打包完成：'
for relative in "${packages[@]}"; do
  print -r -- "  $RELEASE/$relative"
done
print -r -- "檢查碼：$RELEASE/PACKAGES-SHA256SUMS"
print -r -- "簽章狀態：$RELEASE/SIGNING_STATUS.txt"
